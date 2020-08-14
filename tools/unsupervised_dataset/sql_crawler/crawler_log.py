import datetime
import csv
import os
import logging
import pathlib
from sql_crawler import cloud_integration

class CrawlerLog(object):
    """ Logs the status of the SQL crawler, including websites and queries.
        
        The CrawlerLog keeps track of which websites were explored, how many
        queries were found, and creates a CSV with all the queries. It also
        logs any errors encountered. The log is saved into Logs subdirectory
        with name based on start time. Queries are saved into Queries
        subdirectory.
    """

    def __init__(self, stream):
        """ Initializes crawler log to keep track of crawler progress and
            instantiates instance variables.
        """

        self.start_time = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")
        folder_path = str(pathlib.Path(__file__).parent)
        log_folder_path = folder_path + "/logs"
        query_folder_path = folder_path + "/queries"

        # Create directory for logs if it does not already exists
        if not os.path.exists(log_folder_path):
            os.mkdir(log_folder_path)

        logName = "{0}/log-{1}.log".format(log_folder_path, self.start_time)
        logging.basicConfig(filename=logName, filemode="a", level=logging.INFO)
        logging.info("Beginning crawl at time %s.", self.start_time)

        if not os.path.exists(query_folder_path):
            os.mkdir(query_folder_path)

        self.stream = stream
        self.query_name = "{0}/queries_{1}.csv".format(query_folder_path, self.start_time)

        if not self.stream:
            self.csv_file = open(self.query_name, "a")
            self.queries = csv.writer(self.csv_file)
            self.queries.writerow(["Query", "URL"])
        
        self.save_to_gcs = False
        self.save_to_bq = False
        self.batch_data = []
        self.error_log_count = 0

    def log_queries(self, queries, url):
        """ Caches queries to be logged into CSV file or BigQuery. Periodically
            flushes cache and writes queries once reaching maximum size.

        Args:
            queries: Queries to be logged
            url: URL for page containing queries
        """

        self.batch_data += [[query, url] for query in queries]

        while (len(self.batch_data) > 1000):
            self.flush_data(self.batch_data[:1000])
            self.batch_data = self.batch_data[1000:]

    def flush_data(self, data):
        """ Flushes data directly to CSV file or BigQuery.

        Args:
            data: Rows to be flushed to CSV file or BigQuery table
        """

        if self.save_to_bq:
            err = cloud_integration.insert_rows(self.bq_project, self.bq_dataset, self.bq_table, data)
            if err:
                self.log_error(err)

        if not self.stream:
            self.queries.writerows(data)

    def log_page(self, url, count):
        """ Logs results of crawling one page using provided arguments.

        Args:
            url: URL of page being crawled
            count: Number of queries found on the page
        """

        logging.info("Crawled %s. Found %s queries.", url, str(count))

    def log_error(self, errorMessage):
        """ Logs crawler error to logfile.

        Args:
            str: Error message to be logged.
        """

        self.error_log_count += 1
        logging.error("ERROR: %s", errorMessage)

    def parse_location_arg(self, location):
        """ Validates and splits location argument for cloud upload
        into two parts. Should be formatted as project_id.dataset.
        
        Args:
            location: String with name of project ID and dataset.

        Returns
            List of separate strings after splitting location.
        """
        if location.count(".") != 1:
            self.log_error("Argument not formatted correctly: {0}".format(location))
            return None, None

        return location.split(".")

    def set_gcs(self, location):
        """ Sets variables for uploading data to Google Cloud Storage.
 
        Args:
            location: String with name of project ID and bucket name,
            separated by a period.
        """

        self.gcs_project, self.gcs_bucket = self.parse_location_arg(location)
        if self.gcs_project and self.gcs_bucket:
            self.save_to_gcs = True
        
    def set_bq(self, location):
        """ Sets variables for uploading data to Google BigQuery.
            
        Args:
            location: String with name of project ID and dataset name,
            separated by a period.
        """

        self.bq_project, self.bq_dataset = self.parse_location_arg(location)
        self.bq_table = "queries_{0}".format(self.start_time)

        if self.bq_project and self.bq_dataset:
            self.save_to_bq = cloud_integration.create_bigquery_table(self.bq_project, self.bq_dataset, self.bq_table)

        if not self.save_to_bq:
            self.log_error("Unable to create bigquery table.")

    def close(self):
        """ Flushes remaining querise and closes the crawler log. Uploads file
            to Google Cloud. Prints message if there are handled errors logged
            during crawling process.
        """

        logging.info("Finished crawling.")
        
        # Flush remaining queries and close file
        self.flush_data(self.batch_data)
        if not self.stream:
            self.csv_file.close()

        # Save file to GCS, if applicable
        file_name = "queries_{0}".format(self.start_time)
        if self.save_to_gcs:
            status, message = cloud_integration.upload_gcs_file(self.gcs_project,
                self.gcs_bucket, file_name, self.query_name)
            if status:
                logging.info(message)
            else:
                self.log_error(message)

        if self.error_log_count > 0:
            print("Logged {0} errors. See log for details.".format(self.error_log_count))

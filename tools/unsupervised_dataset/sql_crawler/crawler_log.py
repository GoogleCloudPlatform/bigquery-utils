import datetime
import csv
import os
import logging
import pathlib

class CrawlerLog(object):
    """ Logs the status of the SQL crawler, including websites and queries.
        
        The CrawlerLog keeps track of which websites were explored, how many
        queries were found, and creates a CSV with all the queries. It also
        logs any errors encountered. The log is saved into Logs subdirectory
        with name based on start time. Queries are saved into Queries
        subdirectory.
    """

    def __init__(self):
        """ Initializes crawler log to keep track of crawler progress and
            instantiates instance variables.
        """

        self.start_time = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
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

        query_name = "{0}/queries-{1}.csv".format(query_folder_path, self.start_time)
        self.csv_file = open(query_name, "a")
        self.queries = csv.writer(self.csv_file)
        self.queries.writerow(["Query", "URL"])

    def log_query(self, query, url):
        """ Logs query into CSV file.

        Args:
            query: Query to be logged
            url: URL for page containing query
        """

        self.queries.writerow([query, url])

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

        logging.error("ERROR: %s", errorMessage)

    def close(self):
        """ Closes the crawler log.
        """
        logging.info("Finished crawling.")
        self.csv_file.close()

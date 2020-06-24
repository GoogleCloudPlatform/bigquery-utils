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

        self.startTime = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
        folder_path = str(pathlib.Path(__file__).parent)
        log_folder_path = folder_path + "/Logs"
        query_folder_path = folder_path + "/Queries"

        # Create directory for logs if it does not already exists
        if not os.path.exists(log_folder_path):
            os.mkdir(log_folder_path)

        logName = "{0}/log-{1}.log".format(log_folder_path, self.startTime)
        logging.basicConfig(filename=logName, filemode="a", level=logging.INFO)
        logging.info("Beginning crawl at time %s.", self.startTime)

        if not os.path.exists(query_folder_path):
            os.mkdir(query_folder_path)

        queryName = "{0}/queries-{1}.csv".format(query_folder_path, self.startTime)
        self.csvFile = open(queryName, "a")
        self.queries = csv.writer(self.csvFile)
        self.queries.writerow(["Query", "URL"])

    def logQuery(self, query, url):
        """ Logs query into CSV file.

        Args:
            query: Query to be logged
            url: URL for page containing query
        """

        self.queries.writerow([query, url])

    def logPage(self, url, count):
        """ Logs results of crawling one page using provided arguments.

        Args:
            url: URL of page being crawled
            count: Number of queries found on the page
        """

        logging.info("Crawled %s. Found %s queries.", url, str(count))

    def logError(self, errorMessage):
        """ Logs crawler error to logfile.

        Args:
            str: Error message to be logged.
        """

        logging.error("ERROR: %s", errorMessage)

    def close(self):
        """ Closes the crawler log.
        """
        logging.info("Finished crawling.")
        self.csvFile.close()

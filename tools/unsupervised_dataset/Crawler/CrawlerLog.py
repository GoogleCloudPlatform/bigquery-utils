import datetime
import csv
import os
import logging

class CrawlerLog(object):
    """ The CrawlerLog keeps track of which websites were explored, how many
        queries were found, and creates a CSV with all the queries. It also
        logs any errors encountered. The log is saved into Logs subdirectory
        with name based on start time. Queries are saved into Queries
        subdirectory.
    """

    def __init__(self):
        """ Initializes crawler log to keep track of crawler progress and instantiates
            instance variables.
        """

        self.startTime = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")

        # Create directory for logs if it does not already exists
        if not os.path.exists("Logs"):
            os.mkdir("Logs")

        logName = "Logs/log-" + self.startTime + ".log"
        logging.basicConfig(filename=logName, filemode="a", level=logging.INFO)
        logging.info("Beginning crawl at time %s.", self.startTime)

        if not os.path.exists("Queries"):
            os.mkdir("Queries")

        self.csvFile = open("Queries/queries-{0}.csv".format(self.startTime), "a")
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

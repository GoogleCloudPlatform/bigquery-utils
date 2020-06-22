import datetime
import csv
import os

class CrawlerLog:
    """ The CrawlerLog keeps track of which websites were explored, how many queries were found,
        and creates a CSV with all the queries. It also logs any errors encountered. The log is
        saved into Logs subdirectory with name based on start time. Queries are saved into Queries
        subdirectory.
    """
    
    def __init__(self):
        """ Initializes crawler log to keep track of crawler progress and instantiates
            instance variables.
        """
        
        self.startTime = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
        
        # Create directory for logs if it does not already exists
        try:
            os.mkdir("Logs")
        except FileExistsError as e:
            pass
        
        self.log = open("Logs/log-" + self.startTime + ".txt", "a")
        self.log.write("Beginning crawl at time {0}.\n".format(self.startTime))
        
        try:
            os.mkdir("Queries")
        except FileExistsError as e:
            pass
        
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
        
        self.log.write("Crawled {0}. Found {1} queries.\n".format(url, str(count)))
        
    def logError(self, errorMessage):
        """ Logs crawler error to logfile.
        
        Args:
            str: Error message to be logged.
        """
        
        self.log.write("ERROR: {0}\n".format(errorMessage))

    def close(self):
        """ Closes the crawler log.
        """
        
        self.log.close()
        self.csvFile.close()
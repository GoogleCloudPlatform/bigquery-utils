import queue
import requests
import CQNode
import Extractor
import CrawlerLog

class Crawler(object):
    """ Contains the functions and logic to run and coordinate the crawling
        process. Given initial starting URLs and maximum size, the crawler
        will explore websites to look for SQL queries.
    """

    def __init__(self, links, maxDepth=3, maxSize=500):
        """ Initializes the crawler and instance variables.

        Args:
            links: The root URLs to begin crawling from. Can be one or more.
            maxDepth: The maximum depth for the crawler to explore.
            maxSize: THe maximum number of links the crawler should explore.

        """
        self.linkQueue = queue.Queue()
        self.seen = set()
        self.maxDepth = maxDepth
        self.maxSize = maxSize
        self.log = CrawlerLog.CrawlerLog()
        self.count = 0

        for link in links:
            self.linkQueue.put(CQNode.CQNode(link, 0))
            self.seen.add(link)

    def crawl(self):
        """ Begins the crawling process using variables set earlier. Extracts
            queries by locating website-specific HTML tags or searching for
            common expression patterns. Writes queries to output after
            finishing each site.
        """

        while not self.linkQueue.empty():
            # Retrieve the next link in the queue
            nextNode = self.linkQueue.get()
            nodeURL = nextNode.getURL()
            nodeDepth = nextNode.getDepth()

            # Check if crawler has exceeded maximum depth or maximum count
            if nodeDepth >= self.maxDepth or self.count >= self.maxSize:
                self.log.close()
                return

            htmlResponse = self.getHTML(nodeURL)
            if htmlResponse is None:
                continue

            links = Extractor.extractLinks(htmlResponse)
            for link in links:
                self.addNewLink(link, nodeDepth)

            queries = Extractor.extractQueries(htmlResponse)
            for query in queries:
                self.log.logQuery(query, nodeURL)

            self.log.logPage(nodeURL, len(queries))
            self.count += 1

        self.log.close()

    def addNewLink(self, link, parentDepth):
        """ Adds a new link to the queue with increased depth. Checks for
        duplicates against set, and does not add if the link has been seen
        before.

        Args:
            link: The link to be added to the queue.
            parentDepth: The depth of the parent link. The child will be
                added to the queue with parentDepth + 1.
        """

        if link in self.seen:
            return
        self.linkQueue.put(CQNode.CQNode(link, parentDepth+1))
        self.seen.add(link)

    def getHTML(self, url):
        """ Fetches HTML content for a webpage. Logs an error and moves on
            if there is an exception.

        Args:
            url: The url for the webpage being requested.

        Returns:
            An HTML response, or None if there is an exception.
        """
        try:
            req = requests.get(url)
            req.raise_for_status()
            return req
        except requests.exceptions.RequestException as err:
            self.log.logError(str(err))
            return None

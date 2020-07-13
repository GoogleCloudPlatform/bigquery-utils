import queue
import requests
from sql_crawler import cq_node
from sql_crawler import extractor
from sql_crawler import crawler_log

class Crawler(object):
    """ Contains the functions and logic to run and coordinate the crawling
        process. Given initial starting URLs and maximum size, the crawler
        will explore websites to look for SQL queries.
    """

    def __init__(self, links, max_depth=3, max_size=500):
        """ Initializes the crawler and instance variables.

        Args:
            links: The root URLs to begin crawling from. Can be one or more.
            max_depth: The maximum depth for the crawler to explore.
            max_size: THe maximum number of links the crawler should explore.

        """
        self.link_queue = queue.Queue()
        self.seen = set()
        self.max_depth = max_depth
        self.max_size = max_size
        self.log = crawler_log.CrawlerLog()
        self.count = 0

        for link in links:
            self.link_queue.put(cq_node.CQNode(link, 0))
            self.seen.add(link)

    def crawl(self):
        """ Begins the crawling process using variables set earlier. Extracts
            queries by locating website-specific HTML tags or searching for
            common expression patterns. Writes queries to output after
            finishing each site.
        """

        while not self.link_queue.empty():
            # Retrieve the next link in the queue
            next_node = self.link_queue.get()
            node_url = next_node.get_url()
            node_depth = next_node.get_depth()

            # Check if crawler has exceeded maximum depth or maximum count
            if node_depth >= self.max_depth or self.count >= self.max_size:
                self.log.close()
                return

            html_response = self.get_html(node_url)
            if html_response is None:
                continue

            links = extractor.extract_links(html_response)
            for link in links:
                self.add_new_link(link, node_depth)

            queries = extractor.extract_queries(html_response)
            for query in queries:
                self.log.log_query(query, node_url)

            self.log.log_page(node_url, len(queries))
            self.count += 1

        self.log.close()

    def add_new_link(self, link, parent_depth):
        """ Adds a new link to the queue with increased depth. Checks for
        duplicates against set, and does not add if the link has been seen
        before.

        Args:
            link: The link to be added to the queue.
            parent_depth: The depth of the parent link. The child will be
                added to the queue with parentDepth + 1.
        """

        if link in self.seen:
            return
        self.link_queue.put(cq_node.CQNode(link, parent_depth+1))
        self.seen.add(link)

    def get_html(self, url):
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
            self.log.log_error(str(err))
            return None

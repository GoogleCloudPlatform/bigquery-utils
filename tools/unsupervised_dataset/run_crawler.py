""" Script to initialize the SQL crawler on a website of the user's choice """

import sys
from SQLCrawler import Crawler

def start_crawler():
    urls = sys.argv[1:]
    crawler = Crawler.Crawler(urls, maxSize=50)
    crawler.crawl()

def main():
    start_crawler()

if __name__ == "__main__":
    main()

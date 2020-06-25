""" Script to initialize the SQL crawler on a website of the user's choice """

import sys
from sql_crawler import crawler

def start_crawler():
    urls = sys.argv[1:]
    new_crawler = crawler.Crawler(urls, max_size=50)
    new_crawler.crawl()

def main():
    start_crawler()

if __name__ == "__main__":
    main()

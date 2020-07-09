""" Script to initialize the SQL crawler on a website of the user's choice """

import sys
import argparse
from sql_crawler import crawler

def start_crawler():
    parser = argparse.ArgumentParser(description="SQL Web Crawler")
    parser.add_argument("urls", help="A space-separated list of URLs to be crawled", nargs='+')
    parser.add_argument("--max_depth", help="The max depth of the crawler (default=3)", type=int, default=3)
    parser.add_argument("--max_size", help="The maximum number of links to be crawled (default=100)", type=int, default=100)
    args = parser.parse_args()
    new_crawler = crawler.Crawler(args.urls, max_size=args.max_size, max_depth=args.max_depth)
    new_crawler.crawl()

def main():
    start_crawler()

if __name__ == "__main__":
    main()

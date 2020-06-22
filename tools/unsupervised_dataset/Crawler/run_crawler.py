import requests
import sys
import Crawler

# Initializes a crawler and starts the crawling process using command line arguments
def start_crawler():
    # TO-DO: Allow user to optionally define max size or max depth
    urls = sys.argv[1:]
    crawler = Crawler.Crawler(urls, maxSize=100)
    crawler.crawl()
        
def main():
    start_crawler()
    
if __name__ == "__main__":
    main()
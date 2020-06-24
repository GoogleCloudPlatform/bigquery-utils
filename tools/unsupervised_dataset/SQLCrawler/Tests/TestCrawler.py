""" Test Crawler functionality """
import SQLCrawler.Crawler as Crawler

# Test that crawler does not break on invalid HTML request
def test_invalid_request():
    crawler = Crawler.Crawler(["test-url"], maxSize=10)
    assert crawler.getHTML("incorrect-request") is None

# Test that crawler correctly handles duplicate URLs
def test_add_duplicate():
    crawler = Crawler.Crawler(["test-url-1"], maxSize=10)
    assert crawler.linkQueue.qsize() == 1
    crawler.addNewLink("test-url-2", 1)
    assert crawler.linkQueue.qsize() == 2
    crawler.addNewLink("test-url-2", 1)
    assert crawler.linkQueue.qsize() == 2

# Test that crawler correctly handles user arguments
def test_crawler_args():
    crawler = Crawler.Crawler(["test-url"], 5, 10)
    assert crawler.maxDepth == 5
    assert crawler.maxSize == 10

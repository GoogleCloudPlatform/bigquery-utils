""" Test Crawler functionality """
import sql_crawler.crawler as crawler

# Test that crawler does not break on invalid HTML request
def test_invalid_request():
    new_crawler = crawler.Crawler(["test-url"], max_size=10)
    assert new_crawler.get_html("incorrect-request") is None

# Test that crawler correctly handles duplicate URLs
def test_add_duplicate():
    new_crawler = crawler.Crawler(["test-url-1"], max_size=10)
    assert new_crawler.link_queue.qsize() == 1
    new_crawler.add_new_link("test-url-2", 1)
    assert new_crawler.link_queue.qsize() == 2
    new_crawler.add_new_link("test-url-2", 1)
    assert new_crawler.link_queue.qsize() == 2

# Test that crawler correctly handles user arguments
def test_crawler_args():
    new_crawler = crawler.Crawler(["test-url"], 5, 10)
    assert new_crawler.max_depth == 5
    assert new_crawler.max_size == 10

# Test that crawler correctly handles Google Cloud arguments
def test_crawler_cloud_args():
    new_crawler = crawler.Crawler(["test-url"], gcs="gcs-project.gcs-bucket", bq="bq-project.bq-dataset")
    assert new_crawler.log.gcs_project == "gcs-project"
    assert new_crawler.log.gcs_bucket == "gcs-bucket"
    assert new_crawler.log.bq_project == "bq-project"
    assert new_crawler.log.bq_dataset == "bq-dataset"

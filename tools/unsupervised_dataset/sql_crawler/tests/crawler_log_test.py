""" Test CQNode functionality """
import sql_crawler.crawler_log as crawler_log

def test_crawler_log_args():
    new_log = crawler_log.CrawlerLog(stream=True)
    assert(new_log.stream)
    
    new_log.set_bq("test-project.test-dataset")
    assert(new_log.bq_project == "test-project")
    assert(new_log.bq_dataset == "test-dataset")
    
    new_log.set_gcs("test-project.test-bucket")
    assert(new_log.gcs_project == "test-project")
    assert(new_log.gcs_bucket == "test-bucket")
    
def test_add_queries():
    new_log = crawler_log.CrawlerLog(stream=True)
    
    test_queries = ["SELECT * FROM table",
                    "SELECT * FROM table GROUP BY column",
                    "SELECT * FROM table ORDER BY column"]
    
    new_log.log_queries(test_queries, "mock-url")
    assert(len(new_log.batch_data) == 3)
    

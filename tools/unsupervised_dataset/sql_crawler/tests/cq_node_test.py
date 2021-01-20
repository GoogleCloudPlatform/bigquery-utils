""" Test CQNode functionality """
import sql_crawler.cq_node as cq_node

def mock_node():
    node = cq_node.CQNode("https://google.com", 5)
    return node

def test_url():
    assert mock_node().get_url() == "https://google.com"
    
def test_depth():
    assert mock_node().get_depth() == 5
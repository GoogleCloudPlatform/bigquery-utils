""" Test CQNode functionality """
import Crawler.CQNode as CQNode

def mock_node():
    node = CQNode.CQNode("https://google.com", 5)
    return node

def test_url():
    assert mock_node().getURL() == "https://google.com"
    
def test_depth():
    assert mock_node().getDepth() == 5
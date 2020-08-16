""" Test Extractor functionality """
import sql_crawler.extractor as extractor

class MockRequest(object):
    ''' A mock HTML request that contains the text, url, and status_code fields
        found in actual requests
    '''

    def __init__(self, url, status_code, text):
        
        self.url = url
        self.status_code = status_code
        self.text = text

def test_extract_links():
    reader = open("resources/googleCloudSite.html", "r")
    url = "https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax"
    mock_request = MockRequest(url, 200, reader.read())
    assert len(extractor.extract_links(mock_request)) == 501
    
def test_extract_queries():
    reader = open("resources/googleCloudSite.html", "r")
    url = "https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax"
    mock_request = MockRequest(url, 200, reader.read())
    assert len(extractor.extract_queries(mock_request)) == 85
    
def test_generic_extraction():
    reader = open("resources/sample_a.html", "r")
    url = "https://fake.website"
    mock_request = MockRequest(url, 200, reader.read())
    queries = extractor.extract_queries(mock_request)
    assert len(queries) == 5

def test_generic_extraction_with_links():
    reader = open("resources/sample_b.html", "r")
    url = "https://fake.website"
    mock_request = MockRequest(url, 200, reader.read())
    assert len(extractor.extract_links(mock_request)) == 2
    assert len(extractor.extract_queries(mock_request)) == 8

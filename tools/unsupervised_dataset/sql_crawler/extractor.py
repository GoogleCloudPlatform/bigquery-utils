""" Contains the logic to extract links and SQL queries from HTML content.


    This file contains functions to extract links or queries when given an HTML
    response. It also dynamically extracts links based on the URL of the response.
"""
import urllib
import bs4
import sql_crawler.extraction_modules.generic_extraction_module as generic_extraction
import sql_crawler.extraction_modules.google_extraction_module as google_extraction

def extract_links(html):
    """ Extracts links from HTML content of a site.

    Args:
        html: The HTML response which contains the HTML text.

    Returns:
        A list of URLs (strings).
    """

    content = bs4.BeautifulSoup(html.text, "html.parser")
    link_tags = content.find_all("a")
    links = set([])

    for link in link_tags:
        if link.has_attr('href'):
            # Fix relative paths and anchor links
            absolute_path = urllib.parse.urljoin(html.url, link['href'])
            if "#" in absolute_path:
                trimmed = absolute_path.split("#", 1)[0]
                links.add(trimmed)
            else:
                links.add(absolute_path)

    return links

def extract_queries(html):
    """ Extracts queries from HTML content of a site.

    Args:
        html: The HTML response which contains the HTML text.

    Returns:
        A list of queries (strings)
    """

    extractor_module = retrieve_module(html.url)
    return extractor_module.find_queries(html)
    # TODO(Noah): Parse these here before returning

def retrieve_module(url):
    """ Retrieves the correct module to use for extracting queries
    from a specific site. If there is no module for pages under this
    domain, it returns a generic module.

    Args:
        url: The URL for the site being crawled.

    Returns:
        A extraction module, which contains a findQueries function for
        extracting queries.
    """
    if "cloud.google.com" in url:
        return google_extraction.GoogleExtractionModule
    else:
        # TODO(Noah): Add more modules and implement generic module
        return generic_extraction.GenericExtractionModule

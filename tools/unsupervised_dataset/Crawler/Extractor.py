import urllib
import bs4
import ExtractionModules.GenericExtractionModule as GenericExtraction
import ExtractionModules.GoogleExtractionModule as GoogleExtraction

""" Contains the logic to extract links and SQL queries from HTML
    content of a webpage.
"""

def extractLinks(html):
    """ Extracts links from HTML content of a site.

    Args:
        html: The HTML response which contains the HTML text.

    Returns:
        A list of URLs (strings).
    """

    content = bs4.BeautifulSoup(html.text, "html.parser")
    linkTags = content.find_all("a")
    links = set([])

    for link in linkTags:
        if link.has_attr('href'):
            # Fix relative paths and anchor links
            absolutePath = urllib.parse.urljoin(html.url, link['href'])
            if "#" in absolutePath:
                trimmed = absolutePath.split("#", 1)[0]
                links.add(trimmed)
            else:
                links.add(absolutePath)

    return links

def extractQueries(html):
    """ Extracts queries from HTML content of a site.

    Args:
        html: The HTML response which contains the HTML text.

    Returns:
        A list of queries (strings)
    """

    extractorModule = retrieveModule(html.url)
    return extractorModule.findQueries(html)
    # TODO(Noah): Parse these here before returning

def retrieveModule(url):
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
        return GoogleExtraction.GoogleExtractionModule
    else:
        # TODO(Noah): Add more modules and implement generic module
        return GenericExtraction.GenericExtractionModule

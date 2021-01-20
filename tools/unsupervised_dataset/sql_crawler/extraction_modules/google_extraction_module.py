from bs4 import BeautifulSoup
import re

REGEX_SEARCH = r"(?:SELECT|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|EXEC|CALL|USING) (?:(?!;|[.:]\s).)*;"

class GoogleExtractionModule(object):

    def find_queries(html):
        """ Finds queries and extracts them from Google SQL documentation on
        cloud.google.com.

        Code blocks are in <code> tags with parent <pre> tags.

        Args:
            html: HTML response which contains HTML text

        Returns
            A list of queries in the form of strings.
        """

        soup = BeautifulSoup(html.text, "html.parser")
        queries = []
        code_blocks = soup.find_all("code")
        for block in code_blocks:
            if block.parent.name == "pre" and len(block.contents) > 0:
                queries += re.findall(REGEX_SEARCH, block.contents[0], re.DOTALL)
        return queries

""" Collects SQL queries from any website """
import html
import re

REGEX_SEARCH = r"(?:SELECT|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|EXEC|CALL|USING) (?:(?!;|[.:]\s).)*;"

class GenericExtractionModule():
    """ A module to extract SQL queries from any website without knowing the HTML
        format of a site.
    """

    def find_queries(html_response):
        """ Finds queries and extracts them from any website, without using HTML
        tags to locate them.

        Args:
            html: HTML response which contains HTML text

        Returns
            A list of queries in the form of strings.
        """
        # Remove HTML tags and special characters
        content = html_response.text
        converted = html.unescape(content)
        tags_removed = re.sub('<[^<]+?>', '', converted)

        # Look for text that matches common SQL queries
        matches = re.findall(REGEX_SEARCH, tags_removed, re.DOTALL)
        return matches

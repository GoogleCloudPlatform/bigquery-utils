from bs4 import BeautifulSoup
import re

REGEX_SEARCH = r"(?:SELECT|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|EXECUTE|CALL|USING|COPY|ABORT|ANALYZE|CANCEL|CLOSE|COMMENT|COMMIT|DECLARE|DELETE|END|FETCH|GRANT|INSERT|LOCK|PREPARE|REFRESH|RESET|REVOKE|ROLLBACK|SET|SHOW|TRUNCATE|UNLOAD|VACUUM) (?:(?!;|[.:]\s).)*;"
REGEX_COMMENT = r"--.*"

class ExtractionModule2:

    def find_queries(html):
        """ Finds queries and extracts them from websites.

        Args:
            html: HTML response which contains HTML text
            
        Returns
            A list of queries in the form of strings.
        """
        soup = BeautifulSoup(html.text, "html.parser")
        queries = []
        code_blocks = soup.find_all("code")
        for block in code_blocks:
            cleaned_block = re.sub(REGEX_COMMENT, "", str(block.contents[0]))
            queries += re.findall(REGEX_SEARCH, cleaned_block, re.DOTALL|re.IGNORECASE)
        return queries

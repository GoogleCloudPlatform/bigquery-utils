from bs4 import BeautifulSoup
import re

REGEX_SEARCH = r"(?:ABORT|ALTER|ANALYZE|BEGIN|CALL|CHECKPOINT|CLOSE|CLUSTER|COMMENT|COMMIT|COPY|CREATE|DEALLOCATE|DECLARE|DELETE|DISCARD|DO|DROP|EXECUTE|EXPLAIN|FETCH|GRANT|IMPORT|INSERT|LISTEN|LOAD|LOCK|MOVE|NOTIFY|PREPARE|REASSIGN|REFRESH|REINDEX|RELEASE|RESET|REVOKE|ROLLBACK|SAVEPOINT|SELECT|SET|SHOW|START|TRUNCATE|UNLISTEN|UPDATE|VACUUM|VALUES) (?:(?!;|[.:]\s).)*;"
REGEX_COMMENT = r"--.*"

class ExtractionModule4:

    def find_queries(html):
        """ Finds queries and extracts them from websites.

        Args:
            html: HTML response which contains HTML text

        Returns
            A list of queries in the form of strings.
        """

        soup = BeautifulSoup(html.text, "html.parser")
        queries = []
        code_blocks = soup.find_all("pre", class_=["programlisting", "screen"])
        for block in code_blocks:
            if len(block.contents) > 0:
                cleaned_block = re.sub(REGEX_COMMENT, "", str(block.contents[0]))
                queries += re.findall(REGEX_SEARCH, cleaned_block, re.DOTALL)
        return queries

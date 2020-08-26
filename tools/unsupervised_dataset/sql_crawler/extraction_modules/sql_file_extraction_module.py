""" Collects individual SQL queries from a .sql file """
import re

SINGLE_COMMENT = r"--.*"
BLOCK_COMMENT = r"\/\*.*\*\/"


class SQLFileExtractionModule():
    """ A module to extract individual SQL queries from a file, given the contents of the file.
    """

    def find_queries(file_contents):
        """ Finds queries and extracts them from a SQL file.

        Args:
            file_contents: Contents of a .sql file

        Returns
            A list of queries in the form of strings.
        """
        # Remove comments -- and /* */
        cleaned_contents = re.sub(SINGLE_COMMENT, " ", file_contents)
        cleaned_contents = re.sub(BLOCK_COMMENT, " ", cleaned_contents, flags=re.DOTALL)

        # Split according to ; and remove unnecessary whitespace
        split_queries = cleaned_contents.split(";")
        return [re.sub("\s+", " ", query).strip() + ";" for query in split_queries]

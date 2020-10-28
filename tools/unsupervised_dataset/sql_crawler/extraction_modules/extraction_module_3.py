from bs4 import BeautifulSoup

class ExtractionModule3:
    
    def find_queries(html):
        """ Finds queries and extracts them from websites.
        
        Args:
            html: HTML response which contains HTML text
            
        Returns
            A list of queries in the form of strings.
        """
        
        soup = BeautifulSoup(html.text, "html.parser")
        queries = []
        code_blocks = soup.find_all("code", class_="language-sql")
        for block in code_blocks:
            queries += [block.contents[0]]
        return queries
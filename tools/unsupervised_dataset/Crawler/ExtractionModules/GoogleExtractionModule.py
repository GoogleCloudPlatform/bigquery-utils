from bs4 import BeautifulSoup

class GoogleExtractionModule:
    
    def findQueries(html):
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
        codeBlocks = soup.find_all("code")
        for block in codeBlocks:
            if block.parent.name == "pre":
                queries += [block.contents[0]]
        return queries
        
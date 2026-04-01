from psi_author_scraper import scrape_psi_authors
from psi_publication_scraper import scrape_psi_publications
from psi_objectifier import load_authors
from config import DO_SCRAPE_AGAIN

def main(): 
    if DO_SCRAPE_AGAIN: 
        scrape_psi_authors()
        scrape_psi_publications()
    authors = load_authors()
    
if __name__ == "__main__":
    main()
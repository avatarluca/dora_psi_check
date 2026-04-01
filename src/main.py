from psi_author_scraper import scrape_psi_authors
from psi_publication_scraper import scrape_psi_publications
from psi_objectifier import load_authors
from config import DO_SCRAPE_AGAIN

def main(): 
    
    scrape_psi_publications()
    if DO_SCRAPE_AGAIN: 
        scrape_psi_authors()
    authors = load_authors()
    


if __name__ == "__main__":
    main()
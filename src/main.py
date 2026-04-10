from psi_author_scraper import scrape_psi_authors
from psi_publication_scraper import scrape_psi_publications
from psi_objectifier import load_authors
from psi_publication_comparator import run_publication_check
from config import DO_SCRAPE_AGAIN

def main():
    print("[START] PSI publication comparator starting")
    if DO_SCRAPE_AGAIN:
        print("[SCRAPE] Running author and publication scrapers")
        scrape_psi_authors()
        scrape_psi_publications()

    authors = load_authors()
    print(f"[LOAD] Loaded {len(authors)} authors from Excel data")

    results = run_publication_check(authors)

    total = len(results)
    wrong_count = sum(1 for result in results if result.status != "correct")
    print(f"[FINISH] Checked {total} publications. Wrong: {wrong_count}")


if __name__ == "__main__":
    main()
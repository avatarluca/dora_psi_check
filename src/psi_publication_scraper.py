import os
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import DATA_OUTPUT_DIR, PS_FILE_NAME, AS_FILE_NAME, PS_MAX_WORKERS


def load_author_ids(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def fetch_author_publications(author_id: str):
    page = 0
    pub_ids = set()
    
    while True:
        url = (
            f"https://admin.dora.lib4ri.ch/psi/islandora/search"
            f"?page={page}&f[0]=mods_name_personal_author_alternativeName_nameIdentifier_authorId_ms:%22psi\\-authors\\:{author_id}%22"
        )
        response = requests.get(url)
        if response.status_code != 200:
            print(f"[ERROR] page {page} for author {author_id}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        new_ids = set()
        for a in links:
            match = re.search(r'/psi/islandora/object/psi%3A(\d+)', a['href'])
            if match:
                new_ids.add(match.group(1))

        if not new_ids:
            break

        pub_ids.update(new_ids)
        page += 1

    return pub_ids

def save_publications(pub_ids: set):
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(DATA_OUTPUT_DIR, PS_FILE_NAME)

    existing_ids = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            existing_ids = set(line.strip() for line in f if line.strip())

    all_ids = existing_ids.union(pub_ids)
    with open(file_path, "w", encoding="utf-8") as f:
        for pid in sorted(all_ids):
            f.write(f"{pid}\n")

    print(f"{len(pub_ids)} new publications added, total {len(all_ids)}.")

def scrape_psi_publications():
    author_file = os.path.join(DATA_OUTPUT_DIR, AS_FILE_NAME)
    author_ids = load_author_ids(author_file)
    print(f"Loaded {len(author_ids)} author IDs.")

    all_pub_ids = set()

    with ThreadPoolExecutor(max_workers=PS_MAX_WORKERS) as executor:
        future_to_author = {executor.submit(fetch_author_publications, aid): aid for aid in author_ids}
        for future in as_completed(future_to_author):
            author_id = future_to_author[future]
            try:
                pubs = future.result()
                print(f"Author {author_id}: {len(pubs)} publications")
                all_pub_ids.update(pubs)
            except Exception as e:
                print(f"[ERROR] fetching author {author_id}: {e}")

    save_publications(all_pub_ids)
    print("Done fetching all publications.")

if __name__ == "__main__":
    scrape_psi_publications()
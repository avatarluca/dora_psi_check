import requests
from bs4 import BeautifulSoup
import string
import os
import re
import html

from config import AS_BASE_URL, DATA_OUTPUT_DIR, AS_FILE_NAME


def scrape_psi_authors():
    print("Start scraping PSI authors...")
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)

    all_ids = set()  # avoid duplicates

    for letter in string.ascii_uppercase:
        ids = process_letter(letter)
        all_ids.update(ids)

    output_file = os.path.join(DATA_OUTPUT_DIR, AS_FILE_NAME)
    with open(output_file, "w") as f:
        for author_id in sorted(all_ids):
            f.write(author_id + "\n")

    print(f"... done scraping psi authors! ({len(all_ids)} IDs saved)")


def extract_author_ids(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    ids = []

    for a in soup.find_all("a", href=True):
        href = html.unescape(a["href"])

        match = re.search(r'psi\\-authors\\:(\d+)', href)
        if match:
            ids.append(match.group(1))

    return ids


def process_letter(letter):
    page = 0
    collected_ids = []

    while True:
        url = f"{AS_BASE_URL}?letter={letter}&page={page}"
        print(f"Fetching {url}")

        response = requests.get(url)
        if response.status_code != 200:
            break

        ids = extract_author_ids(response.text)

        if not ids:
            break

        collected_ids.extend(ids)
        page += 1

    return collected_ids 


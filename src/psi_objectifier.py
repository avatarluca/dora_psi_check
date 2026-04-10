import csv
import re
import os

from config import OB_INPUT_FILE, DATA_INPUT_DIR, OB_CSV_COLUMN_DISPLAY_NAME, OB_CSV_COLUMN_SOURCE, OB_CSV_COLUMN_GROUP,\
    OB_CSV_COLUMN_SECTION, OB_CSV_COLUMN_LABORATORY, OB_CSV_COLUMN_DIVISION, OB_CSV_COLUMN_LAST_NAME, OB_CSV_COLUMN_FIRST_NAME_INITIAL

from models.author import Entry, Author

def extract_year(source: str):
    match = re.search(r'(\d{4})', source or "")
    return int(match.group(1)) if match else None

def load_authors():
    authors = {}

    input_path = os.path.join(DATA_INPUT_DIR, OB_INPUT_FILE)
    with open(input_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=';')
        reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

        for row in reader:
            row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}

            display_name = row.get(OB_CSV_COLUMN_DISPLAY_NAME)
            if not display_name:
                continue

            entry = Entry(
                year=extract_year(row.get(OB_CSV_COLUMN_SOURCE)),
                gruppe=row.get(OB_CSV_COLUMN_GROUP, ""),
                sektion=row.get(OB_CSV_COLUMN_SECTION, ""),
                lab=row.get(OB_CSV_COLUMN_LABORATORY, ""),
                bereich=row.get(OB_CSV_COLUMN_DIVISION, ""),
            )

            if display_name not in authors:
                authors[display_name] = Author(
                    lastname=row.get(OB_CSV_COLUMN_LAST_NAME, ""),
                    firstname_initial=row.get(OB_CSV_COLUMN_FIRST_NAME_INITIAL, ""),
                    display_name=display_name,
                )

            authors[display_name].add_entry(entry)

    return authors
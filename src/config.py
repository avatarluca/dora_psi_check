# general
DATA_OUTPUT_DIR = "generated_data"
DATA_INPUT_DIR = "data"
DO_SCRAPE_AGAIN = False


# PSI author scraper (AS)
AS_BASE_URL = "https://admin.dora.lib4ri.ch/psi/author-list"
AS_FILE_NAME = "all_author_ids.txt"


# PSI publication scraper (PS)
PS_FILE_NAME = "all_pub_ids.txt"
PS_MAX_WORKERS = 10


# PSI author objectifier
OB_INPUT_FILE = "LATEST_Historische_Gruppen_20260401.csv"
OB_CSV_COLUMN_DISPLAY_NAME = "DisplayName"
OB_CSV_COLUMN_SOURCE = "Source"
OB_CSV_COLUMN_GROUP = "KST Gruppe DORA"
OB_CSV_COLUMN_SECTION = "KST Sektion DORA"
OB_CSV_COLUMN_LABORATORY = "KST Lab DORA"
OB_CSV_COLUMN_DIVISION = "KST Bereich DORA"
OB_CSV_COLUMN_LAST_NAME = "LASTNAME"
OB_CSV_COLUMN_FIRST_NAME_INITIAL = "FirstNameInitial"
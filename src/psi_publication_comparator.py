"""
Main comparator function for checking the affiliation information in the MODS against the Excel data for PSI authors.
The following shows a pseudo code of the main logic of the comparator.

__________________________________________________________________________________________________________________
__________________________________________________________________________________________________________________

PSI_publication_affiliation_check(author_id_list)
__________________________________________________________________________________________________________________
__________________________________________________________________________________________________________________

(1) Is there a date issued field? 
-> No: Then we can't compare the MODS and we break
-> Yes:
    (1.1) For each author_id : author_id_list:
        (1.1.1) Get all publication ids and put it in publication_id_list
    (1.2) Remove duplicates from publication_id_list
    (1.3) For each publication_id : publication_id_list do
        (1.3.1) Get all psi authors in publication as psi_authors
        (1.3.2) Is there no date issued field in this publication or date issued < 2006 or date in last 2 years?
        -> Yes:
            (1.3.2.1) Check if each group is set to "0000 PSI" for each psi author in psi_authors
            (1.3.2.2) If not, mark publication as wrong with "Publication has no date or date is before 2006 but there is still a group that is not 0000 PSI"
            (1.3.2.3) If yes, continue with the next publication in (1.3)
        -> No:
            (1.3.2.1) For each psi_author in psi_authors:
                (1.3.2.1.1) Let D=(group, section, department, division, organizational unit id) from Excel via dataclass for this psi_author and this year
                (1.3.2.1.2) Let M=(group, section, department, division, organizational unit id) from MODS for this psi_author
                (1.3.2.1.3) If D.group is empty (i.e. no entry for this year in Excel for this author): 
                    (1.3.2.1.3.1) Is there a non empty D' before this year?
                    -> Yes:
                        (1.3.2.1.3.1.1) Set D = D' (the most recent! entry before this year) and continue with the next step 
                    -> No:
                        (1.3.2.1.3.1.1) Set D.group to "0000 PSI" and other fields to empty and continue with the next step
                (1.3.2.1.4) If M.group is not empty but M.organizational_unit_id is empty then mark publication as wrong with "There is a group name but no organizational unit id in MODS" and continue with the next psi author in (1.3.2.1)
                (1.3.2.1.5) If M.group is not empty check if M = D (i.e. group, section, department, division, organizational unit id all match)
                -> No:
                    (1.3.2.1.5.1) Mark publication as wrong with "Group information in MODS does not match Excel (potential mismatch)" and continue with the next psi author in (1.3.2.1)
                -> Yes:
                    (1.3.2.1.5.1) This is correct, continue with the next psi author in (1.3.2.1)
__________________________________________________________________________________________________________________


The following shows some special cases and reasoning for the logic above:
(1.3.2)
    $ Publications before 2006: PSI authors have group "0000 PSI"
    $ Publications for the last 2 years must not be checked with Excel
(1.3.2.1.3) 
    § No entry in Excel for this year but there is an entry before this year: Then we take the most recent! entry before this year
    § No entry in Excel for this year and no entry before this year: Then we set group to "0000 PSI" and other fields to empty
(1.3.2.1.4) 
    § Group name is given but not linked with organizational unit in MODS
    
And here is the link to the wiki page as a reference:
https://www.wiki.lib4ri.ch/display/TD/Authors+and+Editors#AuthorsandEditors-Autoren-Verlinkung(FeldStandardizedFormofName)Autoren-Verlinkung(FeldStandardizedFormofName)
"""

from __future__ import annotations

import json
import os
import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from config import AS_FILE_NAME, DATA_OUTPUT_DIR, PS_FILE_NAME, PS_MAX_WORKERS
from psi_publication_scraper import fetch_author_publications, load_author_ids
from models.mods import ModsPublication, ModsAuthor

logger = logging.getLogger(__name__)

def strip_accents(value: Any) -> str:
    text = str(value or "")
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_display_name(value: Any) -> str:
    return re.sub(r"\s+", " ", strip_accents(str(value or "")).strip()).lower()


def get_initials(value: Any) -> str:
    text = strip_accents(str(value or "")).strip()
    if not text:
        return ""
    tokens = re.findall(r"[A-Za-zÄÖÜäöüÀ-ÿ]+", text)
    return "".join(token[0].lower() for token in tokens if token)


def norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def current_year() -> int:
    return date.today().year

def is_last_two_years(pub_year: int, reference_year: Optional[int] = None) -> bool:
    ref = reference_year or current_year()
    return pub_year >= (ref - 2 + 1)

def should_use_0000_only(pub_year: Optional[int], reference_year: Optional[int] = None) -> bool:
    if pub_year is None:
        return True
    return pub_year < 2006 or is_last_two_years(pub_year, reference_year)

def get_attr(entry: Any, *names: str) -> Any:
    for name in names:
        if hasattr(entry, name):
            value = getattr(entry, name)
            if value not in (None, ""):
                return value
    return None

def excel_affiliation_tuple(entry: Any) -> Tuple[str, str, str, str, str]:
    return (
        norm(get_attr(entry, "gruppe", "group")),
        norm(get_attr(entry, "sektion", "section")),
        norm(get_attr(entry, "lab", "department")),
        norm(get_attr(entry, "bereich", "division")),
        norm(get_attr(entry, "organisational_unit_id", "organizational_unit_id", "org_unit_id")),
    )

def mods_affiliation_tuple(author: ModsAuthor) -> Tuple[str, str, str, str, str]:
    return (
        norm(author.group),
        norm(author.section),
        norm(author.department),
        norm(author.division),
        norm(author.org_unit_id),
    )


# TODO: clean this up and put it into a model class 
@dataclass
class PublicationResult:
    pub_id: str
    year: Optional[int]
    status: str
    issues: List[str] = field(default_factory=list)
    authors: List[Dict[str, str]] = field(default_factory=list)
    checked: bool = False


def empty_0000_tuple() -> Tuple[str, str, str, str, str]:
    return ("0000 PSI", "", "", "", "")


def build_display_name(family: str, given: str) -> str:
    return normalize_display_name(f"{family}, {given}")


def affiliation_prefix_tuple(entry: Tuple[str, str, str, str, str]) -> Tuple[str, str, str, str]:
    return tuple(normalize_display_name(value) for value in entry[:4])


def affiliations_match(actual: Tuple[str, str, str, str, str], expected: Tuple[str, str, str, str, str]) -> bool:
    return affiliation_prefix_tuple(actual) == affiliation_prefix_tuple(expected)


def make_soup(text: str, pub_id: str) -> BeautifulSoup:
    for parser in ("lxml-xml", "xml", "html.parser"):
        try:
            soup = BeautifulSoup(text, parser)
            if parser == "html.parser":
                print(f"[PARSER] {pub_id}: falling back to html.parser")
            else:
                print(f"[PARSER] {pub_id}: using {parser}")
            return soup
        except Exception as exc:
            logger.debug("Parser %s unavailable: %s", parser, exc)
    raise RuntimeError("No BeautifulSoup parser available")


def parse_mods_xml(text: str, pub_id: str) -> Optional[ModsPublication]:
    ns = "{http://www.loc.gov/mods/v3}"
    root = ET.fromstring(text)

    date_tag = root.find(f'.//{ns}dateIssued[@keyDate="yes"]')
    year = int(date_tag.text.strip()) if date_tag is not None and norm(date_tag.text).isdigit() else None

    authors: list[ModsAuthor] = []
    name_tags = root.findall(f'.//{ns}name')
    print(f"[PARSE-DEBUG] {pub_id}: found {len(name_tags)} <name> elements")
    for name_tag in name_tags:
        if name_tag.get("type") != "personal":
            continue

        alt_id_tag = name_tag.find(f'.//{ns}nameIdentifier[@type="authorId"]')
        if alt_id_tag is None:
            print(f"[PARSE-DEBUG] {pub_id}: personal name without psi-authors id")
            continue

        alt_id = norm(alt_id_tag.text)
        if not alt_id.startswith("psi-authors:"):
            print(f"[PARSE-DEBUG] {pub_id}: skipped nameIdentifier {alt_id}")
            continue

        psi_author_id = alt_id.split(":", 1)[1].strip()
        family_tag = name_tag.find(f'{ns}namePart[@type="family"]')
        given_tag = name_tag.find(f'{ns}namePart[@type="given"]')
        group_tag = name_tag.find(f'{ns}affiliation[@type="group"]')
        section_tag = name_tag.find(f'{ns}affiliation[@type="section"]')
        department_tag = name_tag.find(f'{ns}affiliation[@type="department"]')
        division_tag = name_tag.find(f'{ns}affiliation[@type="division"]')
        org_unit_tag = name_tag.find(f'{ns}nameIdentifier[@type="organizational unit id"]')

        authors.append(
            ModsAuthor(
                psi_author_id=psi_author_id,
                family=norm(family_tag.text) if family_tag is not None else "",
                given=norm(given_tag.text) if given_tag is not None else "",
                group=norm(group_tag.text) if group_tag is not None else "",
                section=norm(section_tag.text) if section_tag is not None else "",
                department=norm(department_tag.text) if department_tag is not None else "",
                division=norm(division_tag.text) if division_tag is not None else "",
                org_unit_id=norm(org_unit_tag.text) if org_unit_tag is not None else "",
            )
        )

    pub = ModsPublication(pub_id=pub_id, year=year, authors=authors)
    if not hasattr(pub, "wrong_flags"):
        pub.wrong_flags = []
    return pub


def find_excel_author_author_key(authors_dict: dict, mod_author: ModsAuthor, pub_year: Optional[int] = None) -> Optional[str]:
    if mod_author.psi_author_id in authors_dict:
        return mod_author.psi_author_id

    target_name = build_display_name(mod_author.family, mod_author.given)
    for key in authors_dict:
        if normalize_display_name(str(key)) == target_name:
            return key

    family = normalize_display_name(mod_author.family)
    given_initials = get_initials(mod_author.given)
    candidates: List[str] = []
    for key, author in authors_dict.items():
        if normalize_display_name(author.lastname) != family:
            continue

        excel_initials = get_initials(author.firstname_initial)
        if excel_initials and given_initials and excel_initials[0] == given_initials[0]:
            candidates.append(key)

    if not candidates:
        return None
    if len(candidates) == 1 or pub_year is None:
        return candidates[0]

    ranked_candidates: List[Tuple[int, str]] = []
    for candidate in candidates:
        _, source = get_effective_excel_entry(authors_dict[candidate], pub_year)
        score = {
            "exact": 4,
            "previous": 3,
            "synthetic_0000_after_only": 2,
            "synthetic_0000_no_entry": 1,
        }.get(source, 0)
        ranked_candidates.append((score, candidate))

    ranked_candidates.sort(key=lambda item: item[0], reverse=True)
    return ranked_candidates[0][1] if ranked_candidates and ranked_candidates[0][0] > 0 else candidates[0]


def get_author_excel_entry(authors_dict: dict, mod_author: ModsAuthor, pub_year: int) -> Tuple[Optional[Any], str]:
    author_key = find_excel_author_author_key(authors_dict, mod_author, pub_year)
    if author_key is None:
        return None, "missing_author"

    author_obj = authors_dict[author_key]
    return get_effective_excel_entry(author_obj, pub_year)


def get_effective_excel_entry(author_obj: Any, pub_year: int) -> Tuple[Optional[Any], str]:
    entries = list(getattr(author_obj, "entries", []) or [])

    exact = [e for e in entries if getattr(e, "year", None) == pub_year]
    if exact:
        return exact[0], "exact"

    previous = [e for e in entries if getattr(e, "year", None) is not None and e.year < pub_year]
    if previous:
        return max(previous, key=lambda e: e.year), "previous"

    later = [e for e in entries if getattr(e, "year", None) is not None and e.year > pub_year]
    if later:
        return None, "synthetic_0000_after_only"

    return None, "synthetic_0000_no_entry"

def parse_mods(pub_id: str) -> Optional[ModsPublication]:
    url = f"https://admin.dora.lib4ri.ch/psi/islandora/object/psi:{pub_id}/datastream/MODS/view"
    print(f"[FETCH] MODS {pub_id}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Cannot fetch MODS for publication %s: %s", pub_id, exc)
        print(f"[ERROR] Failed to fetch MODS for {pub_id}: {exc}")
        return None

    try:
        pub = parse_mods_xml(response.text, pub_id)
        print(f"[PARSER] {pub_id}: parsed with ET XML parser")
        print(f"[PARSE] Publication {pub_id} parsed, year={pub.year}, authors={len(pub.authors)}")
        for mod_author in pub.authors:
            print(
                f"  [MODS] {pub_id} author={mod_author.psi_author_id} M={mods_affiliation_tuple(mod_author)}"
            )
        return pub
    except Exception as exc:
        print(f"[WARN] XML parser unavailable for {pub_id}, falling back to BeautifulSoup: {exc}")

    soup = make_soup(response.text, pub_id)
    date_tag = soup.find("dateIssued", {"keyDate": "yes"})
    year = int(date_tag.text.strip()) if date_tag and norm(date_tag.text).isdigit() else None

    authors: list[ModsAuthor] = []

    for name_tag in soup.find_all("name", {"type": "personal"}):
        alt_id_tag = name_tag.find("nameIdentifier", {"type": "authorId"})
        if not alt_id_tag:
            continue

        alt_id = norm(alt_id_tag.text)
        if not alt_id.startswith("psi-authors:"):
            continue

        psi_author_id = alt_id.split(":", 1)[1].strip()

        family_tag = name_tag.find("namePart", {"type": "family"})
        given_tag = name_tag.find("namePart", {"type": "given"})

        group_tag = name_tag.find("affiliation", {"type": "group"})
        section_tag = name_tag.find("affiliation", {"type": "section"})
        department_tag = name_tag.find("affiliation", {"type": "department"})
        division_tag = name_tag.find("affiliation", {"type": "division"})
        org_unit_tag = name_tag.find("nameIdentifier", {"type": "organizational unit id"})

        authors.append(
            ModsAuthor(
                psi_author_id=psi_author_id,
                family=norm(family_tag.text) if family_tag else "",
                given=norm(given_tag.text) if given_tag else "",
                group=norm(group_tag.text) if group_tag else "",
                section=norm(section_tag.text) if section_tag else "",
                department=norm(department_tag.text) if department_tag else "",
                division=norm(division_tag.text) if division_tag else "",
                org_unit_id=norm(org_unit_tag.text) if org_unit_tag else "",
            )
        )

    pub = ModsPublication(pub_id=pub_id, year=year, authors=authors)
    if not hasattr(pub, "wrong_flags"):
        pub.wrong_flags = []
    print(f"[PARSE] Publication {pub_id} parsed, year={year}, authors={len(authors)}")
    for mod_author in authors:
        print(
            f"  [MODS] {pub_id} author={mod_author.psi_author_id} M={mods_affiliation_tuple(mod_author)}"
        )
    return pub

def check_publication(pub: ModsPublication, authors_dict: dict, reference_year: Optional[int] = None) -> ModsPublication:
    if not hasattr(pub, "wrong_flags"):
        pub.wrong_flags = []

    pub.checked = True
    pub.status = "correct"

    print(f"[CHECK] Publication {pub.pub_id} year={pub.year}")
    if pub.year is None:
        for mod_author in pub.authors:
            if any(norm(v) for v in [mod_author.group, mod_author.section, mod_author.department, mod_author.division, mod_author.org_unit_id]):
                if mods_affiliation_tuple(mod_author) != empty_0000_tuple():
                    pub.wrong_flags.append(
                        f"Author {mod_author.psi_author_id}: publication has no date, but MODS affiliation is not 0000 PSI"
                    )
        pub.status = "wrong" if pub.wrong_flags else "correct"
        return pub

    if should_use_0000_only(pub.year, reference_year):
        for mod_author in pub.authors:
            mods_tuple = mods_affiliation_tuple(mod_author)
            print(f"  [MODS] {mod_author.psi_author_id} M={mods_tuple}")
            if mods_tuple != empty_0000_tuple():
                pub.wrong_flags.append(
                    f"Author {mod_author.psi_author_id}: publication year {pub.year} is in the 0000 PSI-only branch, "
                    f"but MODS has {mods_tuple}"
                )
        pub.status = "wrong" if pub.wrong_flags else "correct"
        return pub

    for mod_author in pub.authors:
        actual = mods_affiliation_tuple(mod_author)
        print(f"  [MODS] {mod_author.psi_author_id} M={actual}")
        excel_entry, source = get_author_excel_entry(authors_dict, mod_author, pub.year)
        if source == "missing_author":
            print(f"  [EXCEL] {mod_author.psi_author_id}: not found in Excel")
            pub.wrong_flags.append(f"PSI author {mod_author.psi_author_id} not found in Excel dataclass")
            continue

        if excel_entry is None:
            expected = empty_0000_tuple()
            print(f"  [EXCEL] {mod_author.psi_author_id} D=0000 PSI synthetic (no Excel entry)")
        else:
            expected = excel_affiliation_tuple(excel_entry)
            print(f"  [EXCEL] {mod_author.psi_author_id} D={expected} source={source}")

        if norm(mod_author.group) and not norm(mod_author.org_unit_id):
            pub.wrong_flags.append(
                f"Author {mod_author.psi_author_id}: group is set in MODS but organizational unit id is missing"
            )
            continue

        if not affiliations_match(actual, expected):
            pub.wrong_flags.append(
                f"Author {mod_author.psi_author_id}: group information in MODS does not match Excel "
                f"(source={source}, MODS={actual}, Excel={expected})"
            )
        else:
            print(f"  [MATCH] {mod_author.psi_author_id} correct")

    pub.status = "wrong" if pub.wrong_flags else "correct"
    return pub


def load_publication_ids(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Publication id file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def get_unique_publication_ids_from_authors(author_ids: Iterable[str], max_workers: int = PS_MAX_WORKERS) -> List[str]:
    all_pub_ids: set[str] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_author_publications, author_id): author_id for author_id in author_ids}
        for future in as_completed(futures):
            author_id = futures[future]
            try:
                pubs = future.result()
                logger.info("Author %s: %d publications", author_id, len(pubs))
                all_pub_ids.update(pubs)
            except Exception as exc:
                logger.error("Cannot fetch publications for author %s: %s", author_id, exc)
    return sorted(all_pub_ids)


def build_publication_result(pub: Optional[ModsPublication], pub_id: str, error_message: Optional[str] = None) -> PublicationResult:
    if pub is None:
        return PublicationResult(
            pub_id=pub_id,
            year=None,
            status="error",
            issues=[error_message or "Failed to fetch MODS data"],
            checked=True,
        )

    return PublicationResult(
        pub_id=pub.pub_id,
        year=pub.year,
        status=pub.status,
        issues=list(pub.wrong_flags),
        authors=[
            {
                "psi_author_id": a.psi_author_id,
                "family": a.family,
                "given": a.given,
                "group": a.group,
                "section": a.section,
                "department": a.department,
                "division": a.division,
                "org_unit_id": a.org_unit_id,
            }
            for a in pub.authors
        ],
        checked=pub.checked,
    )


def save_publication_report(results: List[PublicationResult], data_output_dir: str, file_name: str = "publication_check_report.json") -> str:
    os.makedirs(data_output_dir, exist_ok=True)
    file_path = os.path.join(data_output_dir, file_name)

    json_list = [
        {
            "pub_id": result.pub_id,
            "year": result.year,
            "status": result.status,
            "checked": result.checked,
            "issues": result.issues,
            "authors": result.authors,
        }
        for result in results
    ]

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_list, f, indent=2, ensure_ascii=False)

    logger.info("Saved publication report for %s publications to %s", len(results), file_path)
    return file_path


def run_publication_check(authors_dict: dict, publication_ids: Optional[Iterable[str]] = None, author_ids: Optional[Iterable[str]] = None, max_workers: int = PS_MAX_WORKERS) -> List[PublicationResult]:
    if publication_ids is None:
        pub_id_file = os.path.join(DATA_OUTPUT_DIR, PS_FILE_NAME)
        if os.path.exists(pub_id_file):
            publication_ids = load_publication_ids(pub_id_file)
            logger.info("Loaded %s publication IDs from %s", len(publication_ids), pub_id_file)
        else:
            if author_ids is None:
                author_ids = load_author_ids(os.path.join(DATA_OUTPUT_DIR, AS_FILE_NAME))
            publication_ids = get_unique_publication_ids_from_authors(author_ids, max_workers)

    publication_ids = list(dict.fromkeys(publication_ids))
    results: List[PublicationResult] = []

    print(f"[RUN] Checking {len(publication_ids)} publications with {max_workers} workers")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(parse_mods, pub_id): pub_id for pub_id in publication_ids}
        for future in as_completed(futures):
            pub_id = futures[future]
            try:
                pub = future.result()
                if pub is None:
                    print(f"[RESULT] {pub_id}: error - failed to fetch MODS")
                    results.append(build_publication_result(None, pub_id, f"Failed to fetch MODS for publication {pub_id}"))
                    continue
                checked_pub = check_publication(pub, authors_dict)
                results.append(build_publication_result(checked_pub, pub_id))
                print(f"[RESULT] {pub_id}: {checked_pub.status} ({len(checked_pub.wrong_flags)} issues)")
                if checked_pub.status != "correct":
                    for issue in checked_pub.wrong_flags:
                        print(f"  - {issue}")
                logger.info("Checked publication %s: %s", pub_id, checked_pub.status)
            except Exception as exc:
                print(f"[RESULT] {pub_id}: error - {exc}")
                logger.error("Error checking publication %s: %s", pub_id, exc)
                results.append(build_publication_result(None, pub_id, str(exc)))

    report_path = save_publication_report(results, DATA_OUTPUT_DIR)
    print(f"[SAVE] Report saved to {report_path}")
    return results


def save_wrong_publications(wrong_pubs: list, data_output_dir: str, file_name: str = "wrong_pubs.json") -> str:
    os.makedirs(data_output_dir, exist_ok=True)
    file_path = os.path.join(data_output_dir, file_name)

    json_list = []
    for pub in wrong_pubs:
        if getattr(pub, "wrong_flags", None):
            json_list.append(
                {
                    "pub_id": pub.pub_id,
                    "year": pub.year,
                    "issues": pub.wrong_flags,
                    "authors": [
                        {
                            "psi_author_id": a.psi_author_id,
                            "family": a.family,
                            "given": a.given,
                            "group": a.group,
                            "section": a.section,
                            "department": a.department,
                            "division": a.division,
                            "org_unit_id": a.org_unit_id,
                        }
                        for a in pub.authors
                    ],
                }
            )

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_list, f, indent=2, ensure_ascii=False)

    logger.info("Saved %s wrong publications to %s", len(json_list), file_path)
    return file_path
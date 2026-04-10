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
from datetime import date
from typing import Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from models.mods import ModsPublication, ModsAuthor

logger = logging.getLogger(__name__)

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
    """Return the first existing attribute value among several possible names."""
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

def empty_0000_tuple() -> Tuple[str, str, str, str, str]:
    return ("0000 PSI", "", "", "", "")

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

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Cannot fetch MODS for publication %s: %s", pub_id, exc)
        return None

    soup = BeautifulSoup(response.text, "xml")

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
    return pub

def check_publication(pub: ModsPublication, authors_dict: dict, reference_year: Optional[int] = None) -> ModsPublication:
    if not hasattr(pub, "wrong_flags"):
        pub.wrong_flags = []

    if pub.year is None:
        for mod_author in pub.authors:
            if any(norm(v) for v in [mod_author.group, mod_author.section, mod_author.department, mod_author.division, mod_author.org_unit_id]):
                if mods_affiliation_tuple(mod_author) != empty_0000_tuple():
                    pub.wrong_flags.append(
                        f"Author {mod_author.psi_author_id}: publication has no date, but MODS affiliation is not 0000 PSI"
                    )
        return pub

    if should_use_0000_only(pub.year, reference_year):
        for mod_author in pub.authors:
            mods_tuple = mods_affiliation_tuple(mod_author)
            if mods_tuple != empty_0000_tuple():
                pub.wrong_flags.append(
                    f"Author {mod_author.psi_author_id}: publication year {pub.year} is in the 0000 PSI-only branch, "
                    f"but MODS has {mods_tuple}"
                )
        return pub

    for mod_author in pub.authors:
        if mod_author.psi_author_id not in authors_dict:
            pub.wrong_flags.append(f"PSI author {mod_author.psi_author_id} not found in Excel dataclass")
            continue

        author_obj = authors_dict[mod_author.psi_author_id]
        excel_entry, source = get_effective_excel_entry(author_obj, pub.year)

        if excel_entry is None:
            expected = empty_0000_tuple()
        else:
            expected = excel_affiliation_tuple(excel_entry)

        actual = mods_affiliation_tuple(mod_author)

        if norm(mod_author.group) and not norm(mod_author.org_unit_id):
            pub.wrong_flags.append(
                f"Author {mod_author.psi_author_id}: group is set in MODS but organizational unit id is missing"
            )
            continue

        if actual != expected:
            pub.wrong_flags.append(
                f"Author {mod_author.psi_author_id}: group information in MODS does not match Excel "
                f"(source={source}, MODS={actual}, Excel={expected})"
            )

    return pub

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
"""
Then we directly do the following for each pub:
We go to mods: https://admin.dora.lib4ri.ch/psi/islandora/object/psi:[ID like 64533]/datastream/MODS/view
In there we see entries like 

<name type="personal">
<namePart type="family">Pautz</namePart>
<namePart type="given">A.</namePart>
<role>
<roleTerm authority="marcrelator" type="text">author</roleTerm>
</role>
<alternativeName altType="formal_name">
<namePart>Andreas Pautz</namePart>
<nameIdentifier type="authorId">psi-authors:1336</nameIdentifier>
</alternativeName>
<nameIdentifier type="organizational unit id">psi-units:90</nameIdentifier>
<affiliation type="group">4000 Nuclear Engineering and Sciences</affiliation>
<affiliation type="section"/>
<affiliation type="department"/>
<affiliation type="division">Nuclear Engineering and Sciences NES</affiliation>
</name> 
where we see that he is psi author but sometimes also other authors like 
<name type="personal">
<namePart type="family">Clarke</namePart>
<namePart type="given">S.D.</namePart>
<role>
<roleTerm authority="marcrelator" type="text">author</roleTerm>
</role>
<alternativeName altType="formal_name">
<namePart/>
<nameIdentifier type="authorId"/>
</alternativeName>
<nameIdentifier type="organizational unit id"/>
<affiliation type="group"/>
<affiliation type="section"/>
<affiliation type="department"/>
<affiliation type="division"/>
</name>
which we can ignor (we just intrest us for psi authors)
we also have publication year with <originInfo>
<dateIssued encoding="w3cdtf" keyDate="yes">2025</dateIssued>
</originInfo> 

Then we have this dataclasses which we created (with authors)


Now we have to develop the follwoing algorithm:
In the end we want wrong publications with the following defined criteria

- If there is not a date issued field we mark this publication with "no date and not comparable
- If there is we do the following:
   For each of the defined psi authors in the mods we get group, section, department, division, organizational unit id
   Then we get the entry of the author dataclass of this author for this certain year.
   If there is no entry in the dataclass for this year and group isnt "0000 PSI" we mark this publication as wrong with "There is no entry in Excel but Author is still in a group not 0000 PSI"
   If there is 0000 PSI then this is correct
   Then if there is a group name but not a orgianizational unit id we also mark it as wrong with a text
   If the group name (especially the id of the group which is in the mods the first number in the group thingy, is not equal to the excel group in this year (excel has priority)) Then do the following:
   Mark it as wrong but with the right group id (like sugesstion like "shouldnt it be....?") And if the group is right but the laboratory items and division and other thingys not the also 
   Even if already pub is set to wrong of a prev author of this pub still do this for every author of this pub who is psi

   Put it together in a nice way maybe nice json file or something
"""

import requests
from bs4 import BeautifulSoup
import json

from models.mods import ModsPublication, ModsAuthor

def parse_mods(pub_id: str) -> ModsPublication:
    url = f"https://admin.dora.lib4ri.ch/psi/islandora/object/psi:{pub_id}/datastream/MODS/view"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"[ERROR] Cannot fetch MODS for publication {pub_id}")
        return None

    soup = BeautifulSoup(response.text, "xml")
    
    date_tag = soup.find("dateIssued", {"keyDate": "yes"})
    year = int(date_tag.text) if date_tag and date_tag.text.isdigit() else None

    authors = []
    for name_tag in soup.find_all("name", {"type": "personal"}):
        alt_id_tag = name_tag.find("nameIdentifier", {"type": "authorId"})
        if not alt_id_tag or not alt_id_tag.text.startswith("psi-authors:"):
            continue  # skip non-psi authors

        psi_author_id = alt_id_tag.text.split(":")[1].strip()
        family = name_tag.find("namePart", {"type": "family"}).text if name_tag.find("namePart", {"type": "family"}) else ""
        given = name_tag.find("namePart", {"type": "given"}).text if name_tag.find("namePart", {"type": "given"}) else ""

        group = name_tag.find("affiliation", {"type": "group"}).text.strip() if name_tag.find("affiliation", {"type": "group"}) else None
        section = name_tag.find("affiliation", {"type": "section"}).text.strip() if name_tag.find("affiliation", {"type": "section"}) else None
        department = name_tag.find("affiliation", {"type": "department"}).text.strip() if name_tag.find("affiliation", {"type": "department"}) else None
        division = name_tag.find("affiliation", {"type": "division"}).text.strip() if name_tag.find("affiliation", {"type": "division"}) else None
        org_unit_id_tag = name_tag.find("nameIdentifier", {"type": "organizational unit id"})
        org_unit_id = org_unit_id_tag.text.strip() if org_unit_id_tag else None

        authors.append(ModsAuthor(
            psi_author_id=psi_author_id,
            family=family,
            given=given,
            group=group,
            section=section,
            department=department,
            division=division,
            org_unit_id=org_unit_id
        ))

    return ModsPublication(pub_id=pub_id, year=year, authors=authors)

def check_publication(pub: ModsPublication, authors_dict: dict):
    if not pub.year:
        pub.wrong_flags.append("No date issued, not comparable")
        return pub

    for mod_author in pub.authors:
        if mod_author.psi_author_id not in authors_dict:
            pub.wrong_flags.append(f"PSI Author {mod_author.psi_author_id} not in dataclass")
            continue

        author_obj = authors_dict[mod_author.psi_author_id]
        entry_for_year = next((e for e in author_obj.entries if e.year == pub.year), None)

        # if no entry for this year
        if not entry_for_year:
            if mod_author.group != "0000 PSI":
                pub.wrong_flags.append(
                    f"Author {mod_author.psi_author_id} is in group '{mod_author.group}' but has no Excel entry for {pub.year}"
                )
            continue

        if mod_author.group and not mod_author.org_unit_id:
            pub.wrong_flags.append(
                f"Author {mod_author.psi_author_id} has group '{mod_author.group}' but no org_unit_id"
            )

        if entry_for_year.gruppe and mod_author.group:
            excel_group_id = entry_for_year.gruppe.split()[0]  # first number
            mods_group_id = mod_author.group.split()[0]
            if excel_group_id != mods_group_id:
                pub.wrong_flags.append(
                    f"Author {mod_author.psi_author_id} group mismatch: MODS '{mod_author.group}' vs Excel '{entry_for_year.gruppe}'"
                )

        for field_name in ["sektion", "lab", "bereich"]:
            excel_value = getattr(entry_for_year, field_name)
            mods_value = getattr(mod_author, {"sektion": "section", "lab": "department", "bereich": "division"}[field_name])
            if excel_value and mods_value and excel_value != mods_value:
                pub.wrong_flags.append(
                    f"Author {mod_author.psi_author_id} {field_name} mismatch: MODS '{mods_value}' vs Excel '{excel_value}'"
                )

    return pub

def save_wrong_publications(wrong_pubs: list, file_name="wrong_pubs.json"):
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(DATA_OUTPUT_DIR, file_name)

    json_list = []
    for pub in wrong_pubs:
        if pub.wrong_flags:
            json_list.append({
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
                        "division": a.division
                    } for a in pub.authors
                ]
            })

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_list, f, indent=2)

    print(f"Saved {len(json_list)} wrong publications to {file_path}")
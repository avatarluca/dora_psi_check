import os
import sys
import unittest
from unittest.mock import mock_open, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from models.mods import ModsAuthor, ModsPublication
from psi_objectifier import load_authors
from psi_publication_comparator import (
    check_publication,
    get_author_excel_entry,
    run_publication_check,
)

# Mocks

CSV_HEADER = (
    "Source;LASTNAME;FirstNameInitial;DisplayName;SAP_Combined;"
    "KST Gruppe DORA;KST Sektion DORA;KST Lab DORA;KST Bereich DORA;CID_ID;"
    "Bemerkung DORA-Team;Comments Laura 4.3\n"
)

CSV_ROWS = (
    "SAP2021;Emmenegger;M.;Emmenegger, M.;;9321 Power Electronics;Power Electronics;"
    "Infrastructure and Electrical Installations AIE;Corporate Services CCS;;;\n"
    "SAP2007;Emmenegger;M.;Emmenegger, M.;;9320 Power Electronics;Power Electronics;"
    "Infrastructure and Electrical Installations AIE;Corporate Services CCS;;;\n"
    "SAP2006;Jäckle;Hans;Jäckle, Hans;;9320 Power Electronics;Power Electronics;"
    "Infrastructure and Electrical Installations AIE;Corporate Services CCS;;;\n"
    "SAP2011;Künzi;René;Künzi, René;;9321 Power Electronics;Power Electronics;"
    "Infrastructure and Electrical Installations AIE;Corporate Services CCS;;;\n"
    "SAP2010;Künzi;René;Künzi, René;;9320 Power Electronics;Power Electronics;"
    "Infrastructure and Electrical Installations AIE;Corporate Services CCS;;;\n"
    "SAP2021;Richner;Simon;Richner, Simon;;8422 Power Electronics Engineering;Power Electronics;"
    "Accelerator Technology ABT;Accelerator Science and Engineering CAS;;;\n"
)

CSV_ALL_AUTHORS = CSV_HEADER + CSV_ROWS


class TestPublicationComparator(unittest.TestCase):
    def test_recent_author_before_publication_date_uses_previous_entry(self):
        """
            Tests that if the most recent author entry before the publication year is used, 
            even if there is a more recent entry after the publication year. 
            
            This is done by checking the publication with the author "Emmenegger, M." who has entries in 2007 and 2021, and the publication year is 2010.
            The expected behavior is that the 2007 entry is used, since the 2021 entry is after the publication year.
        """
        with patch("psi_objectifier.open", mock_open(read_data=CSV_ALL_AUTHORS), create=True):
            authors = load_authors()

        mod_author = ModsAuthor(
            psi_author_id="3967",
            family="Emmenegger",
            given="M.",
            group="8420 Power Electronics",
            section="Power Electronics",
            department="Infrastructure and Electrical Installations AIE",
            division="Corporate Services CCS",
            org_unit_id="psi-units:261",
        )

        entry, source = get_author_excel_entry(authors, mod_author, 2010)
        console_output = f"Source: {source}, Entry: {entry}"
        print(console_output)

        self.assertEqual(source, "previous")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.year, 2007)
        self.assertEqual(entry.gruppe, "9320 Power Electronics")

    def test_no_entry_before_date_returns_0000_psi(self):
        """
            Tests that if there is no author entry before the publication year, 
            then the synthetic 0000 entry is used.

            This is done by checking the publication with the author "Richner, Simon" who has an entry in 2021,
            without any entry before 2010. The expected behavior is that the synthetic 0000 entry is used, since there is no entry before the publication year.
        """
        with patch("psi_objectifier.open", mock_open(read_data=CSV_ALL_AUTHORS), create=True):
            authors = load_authors()

        mod_author = ModsAuthor(
            psi_author_id="5367",
            family="Richner",
            given="Simon",
            group="8420 Power Electronics",
            section="Power Electronics",
            department="Accelerator Technology ABT",
            division="Accelerator Science and Engineering CAS",
            org_unit_id="psi-units:261",
        )

        entry, source = get_author_excel_entry(authors, mod_author, 2010)

        self.assertIsNone(entry)
        self.assertEqual(source, "synthetic_0000_after_only")

    def test_exact_year_entry_is_used_when_present(self):
        """
            Tests that if there is an author entry with the exact publication year, then this entry is used.
            This is done by checking the publication with the author "Künzi, René" who has an entry in 2010, 
            which is the same as the publication year. Even if there is a more recent entry in 2011, the 2010 entry should be used 
            since it matches the publication year exactly.

            This is an important test to ensure that the comparator correctly identifies and uses the entry that matches the publication year, 
            rather than defaulting to the most recent entry or the previous entry.
        """
        with patch("psi_objectifier.open", mock_open(read_data=CSV_ALL_AUTHORS), create=True):
            authors = load_authors()

        mod_author = ModsAuthor(
            psi_author_id="4893",
            family="Künzi",
            given="René",
            group="9320 Power Electronics",
            section="Power Electronics",
            department="Infrastructure and Electrical Installations AIE",
            division="Corporate Services CCS",
            org_unit_id="psi-units:261",
        )

        entry, source = get_author_excel_entry(authors, mod_author, 2010)

        self.assertEqual(source, "exact")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.year, 2010)
        self.assertEqual(entry.gruppe, "9320 Power Electronics")

    def test_psi_units_are_ignored_in_affiliation_comparison(self):
        """
            Tests that the PSI unit information is ignored when comparing the affiliation information between the MODS data and the Excel data.
            
            This is done by checking the publication with the author "Künzi, René" who has an entry in 2010 with group "9320 Power Electronics", and the MODS data has the same group but with a different org_unit_id.
            The expected behavior is that the comparator ignores the difference in org_unit_id and considers the affiliation
        """

        csv_body = (
            CSV_HEADER
            + "SAP2010;Künzi;René;Künzi, René;;8420 Power Electronics;Power Electronics;"
            "Accelerator Technology ABT;Accelerator Science and Engineering CAS;;;\n"
        )
        with patch("psi_objectifier.open", mock_open(read_data=csv_body), create=True):
            authors = load_authors()

        mod_author = ModsAuthor(
            psi_author_id="4893",
            family="Künzi",
            given="René",
            group="8420 Power Electronics",
            section="Power Electronics",
            department="Accelerator Technology ABT",
            division="Accelerator Science and Engineering CAS",
            org_unit_id="psi-units:261",
        )

        pub = ModsPublication(pub_id="64111", year=2010, authors=[mod_author])
        checked = check_publication(pub, authors)

        self.assertEqual(checked.status, "correct")
        self.assertEqual(checked.wrong_flags, [])

    @patch("psi_publication_comparator.load_publication_ids", return_value=["64111"])
    @patch("psi_publication_comparator.os.path.exists", return_value=True)
    @patch("psi_publication_comparator.parse_mods")
    @patch("psi_publication_comparator.save_publication_report", return_value="dummy_report.json")
    def test_run_publication_check_loads_all_pub_ids_and_csv(self, mock_save_report, mock_parse_mods, mock_exists, mock_pub_ids):
        with patch("psi_objectifier.open", mock_open(read_data=CSV_ALL_AUTHORS), create=True):
            authors = load_authors()

        mock_parse_mods.return_value = ModsPublication(
            pub_id="64111",
            year=2010,
            authors=[
                ModsAuthor(
                    psi_author_id="3967",
                    family="Emmenegger",
                    given="M.",
                    group="8420 Power Electronics",
                    section="Power Electronics",
                    department="Infrastructure and Electrical Installations AIE",
                    division="Corporate Services CCS",
                    org_unit_id="psi-units:261",
                )
            ],
        )

        results = run_publication_check(authors)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].pub_id, "64111")
        self.assertEqual(results[0].year, 2010)
        self.assertTrue(results[0].checked)
        mock_save_report.assert_called_once()

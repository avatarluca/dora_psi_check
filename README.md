# PSI Affiliation Check

This repository contains the code for the PSI Affiliation Check, which is a tool to compare the affiliation information of authors in the MODS data with the affiliation information in the CSV data. The tool identifies discrepancies and generates a report of the findings.

## Usage
To run the app locally just use `python src/main.py` from the root of the repository. Make sure to have the required CSV files in the `data/input` directory (which should be the case if you cloned the repository because the files are distributed with the code). <br/>

You can also configure things in the `config.py` file, such as the input and output directories, and the names of the input files.

## Testing
To run the tests, use `python -m unittest discover -s tests` from the root of the repository. The tests are designed to check the functionality of the publication comparator, ensuring that it correctly identifies discrepancies in affiliation information and handles various edge cases. This is done by using mock data and patching the file reading functions to simulate different scenarios. 

## Comparison Logic
Please see the comment of the pseudo code and used rules in [psi_publication_comparator.py](src/psi_publication_comparator.py) for more details on the comparison logic and the rules that are applied when comparing the affiliation information between the MODS data and the Excel data.

## TODO
- Add more tests 
- Add generation of wrong affiliations report dynamically while comparing the data instead of waiting until the end to generate the report
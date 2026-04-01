from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Entry:
    year: Optional[int]
    gruppe: str
    sektion: str
    lab: str
    bereich: str


@dataclass
class Author:
    lastname: str
    firstname_initial: str
    display_name: str
    entries: List[Entry] = field(default_factory=list)

    def add_entry(self, entry: Entry):
        self.entries.append(entry)

    def sort_by_year(self):
        self.entries.sort(key=lambda x: x.year or 0)

    def get_latest_entry(self) -> Optional[Entry]:
        if not self.entries:
            return None
        return max(self.entries, key=lambda x: x.year or 0)

    def get_unique_bereiche(self):
        return set(e.bereich for e in self.entries if e.bereich)
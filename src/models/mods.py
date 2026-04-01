from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class ModsAuthor:
    psi_author_id: str
    family: str
    given: str
    group: Optional[str] = None
    section: Optional[str] = None
    department: Optional[str] = None
    division: Optional[str] = None
    org_unit_id: Optional[str] = None

@dataclass
class ModsPublication:
    pub_id: str
    year: Optional[int] = None
    authors: List[ModsAuthor] = field(default_factory=list)
    wrong_flags: List[str] = field(default_factory=list)
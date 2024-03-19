from dataclasses import dataclass
from typing import List

@dataclass
class FieldMetadata:
    field_name: str
    description: str

@dataclass
class ReconMetadata:
    recon_name: str
    covered_fields: List[FieldMetadata]
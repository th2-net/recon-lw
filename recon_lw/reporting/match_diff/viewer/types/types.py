from dataclasses import dataclass
from typing import Union, List, Any


@dataclass
class MatchDiffExampleData:
    message_id: str
    message_content: Union[List[Any], Any]
from abc import abstractmethod, ABC
from typing import List, Any

from recon_lw.core.type.types import Message


class IExampleContentProvider(ABC):
    """
    This class should return messages content that will be shown in the examples.

    E.g.
    -----------------------
    Failed field: A  1 != 2
    ------------------------
    Content         |   Content
    Example msg 1   |   Example msg 2
    """
    @abstractmethod
    def get_example_content(self, ids: List[str], messages: List[Message]) -> List[Any]:
        pass

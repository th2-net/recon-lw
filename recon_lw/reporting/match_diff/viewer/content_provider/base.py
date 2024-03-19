from abc import abstractmethod, ABC
from typing import List, Any

from recon_lw.core.type.types import Message


class IExampleContentProvider(ABC):
    @abstractmethod
    def get_example_content(self, ids: List[str], messages: List[Message]) -> List[Any]:
        pass
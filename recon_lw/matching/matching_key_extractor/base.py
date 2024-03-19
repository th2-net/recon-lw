from abc import ABC, abstractmethod
from typing import Any, Dict, Set, List, Protocol
from recon_lw.interpretation.adapter.base import Adapter

class MatchingKeyExtractor(ABC):

    def __call__(self, adapter: Adapter, message: Dict[str, Any], fields: Set[str]) -> List[str]:
        return self.extract(adapter, message, fields)

    @abstractmethod
    def extract(self, adapter: Adapter, message: Dict[str, Any], fields: Set[str]) -> List[str]:
        pass

class MatchingKeyExtractorProtocol(Protocol):
    def __call__(self, adapter: Adapter, message: Dict[str, Any], fields: Set[str]) -> List[str]:
        pass

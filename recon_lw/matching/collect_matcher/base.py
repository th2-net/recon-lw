from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Protocol
from recon_lw.core.rule.base import AbstractRule
from recon_lw.core.type.types import Message


class CollectMatcher(ABC):
    """
    Abstract base class for defining a matching flow.

    This class defines the interface for matcher implementations that collect
    matches between messages based on a given rule.

    Methods:
        collect_matches: Abstract method to collect matches based on a rule.
    """

    def __call__(self, batch: List[Optional[Dict]], rule: AbstractRule):
        return self.collect_matches(batch, rule)

    @abstractmethod
    def collect_matches(self, batch: List[Optional[Dict]], rule: AbstractRule):
        pass

class CollectMatcherProtocol(Protocol):
    def __call__(self, batch: List[Message], state: dict):
        pass
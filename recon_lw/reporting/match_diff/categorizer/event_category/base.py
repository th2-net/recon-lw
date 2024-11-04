from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Protocol, Optional

from recon_lw.reporting.known_issues import Issue


@dataclass(frozen=True)
class EventCategory:
    name: str
    issue: Optional[Issue] = None
    field: Optional[str] = None


class ICategoryExtractor(ABC):

    @abstractmethod
    def extract_diff_category(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        pass

    @abstractmethod
    def extract_miss_category(self, recon_name: str, event: dict) -> EventCategory:
        pass


class IEventCategoryExtractorProtocol(Protocol):
    def __call__(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        pass


@dataclass
class ErrorCategoryStrategy:
    match_extractor: IEventCategoryExtractorProtocol
    match_diff_extractor: IEventCategoryExtractorProtocol
    miss_left_extractor: IEventCategoryExtractorProtocol
    miss_right_extractor: IEventCategoryExtractorProtocol
    category_extractor: ICategoryExtractor

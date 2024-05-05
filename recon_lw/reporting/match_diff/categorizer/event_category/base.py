from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class EventCategory:
    name: str


class IEventCategoryExtractor(ABC):

    def __call__(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        return self.extract_category(recon_name, orig, copy, event)

    @abstractmethod
    def extract_category(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        pass


class IEventCategoryExtractorProtocol(Protocol):
    def __call__(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        pass


class IDiffCategoryExtractor(ABC):
    def __call__(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        return self.extract_category(recon_name, diff, event)

    @abstractmethod
    def extract_category(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        pass


class IDiffCategoryExtractorProtocol(Protocol):

    def __call__(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        pass


@dataclass
class ErrorCategoryStrategy:
    match_extractor: IEventCategoryExtractorProtocol
    match_diff_extractor: IEventCategoryExtractorProtocol
    miss_left_extractor: IEventCategoryExtractorProtocol
    miss_right_extractor: IEventCategoryExtractorProtocol
    diff_category_extractor: IDiffCategoryExtractorProtocol

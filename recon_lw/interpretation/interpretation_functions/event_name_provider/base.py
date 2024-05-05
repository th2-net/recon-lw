from abc import ABC, abstractmethod
from typing import Protocol

from recon_lw.interpretation.interpretation_functions import ReconType


class ReconEventNameProvider(ABC):
    @abstractmethod
    def get_miss_original_event_name(self):
        pass

    @abstractmethod
    def get_miss_copy_event_name(self):
        pass

    @abstractmethod
    def get_match_event_name(self):
        pass

    @abstractmethod
    def get_match_diff_event_name(self):
        pass


class ReconEventNameProviderProtocol(Protocol):
    def __call__(self, event_type: ReconType, successful: bool = True):
        pass

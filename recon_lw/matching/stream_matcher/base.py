from abc import ABC, abstractmethod

from recon_lw.core.rule import AbstractRule
from recon_lw.core.utility.recon_utils import *


class ReconMatcher(ABC):
    @abstractmethod
    def match(self, next_batch: List[Optional[Dict]], rule: AbstractRule):
        pass
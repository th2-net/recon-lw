from abc import ABC, abstractmethod

from recon_lw.matching.init_function.context.base import AbstractMatcherContext


class MatcherContextProvider(ABC):
    @abstractmethod
    def get_context(self) -> AbstractMatcherContext:
        pass
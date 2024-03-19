from abc import abstractmethod, ABC
from typing import List

from recon_lw.reporting.match_diff.categorizer.types import ReconErrorStatsContext
from recon_lw.reporting.match_diff.categorizer.types import ErrorCategoriesStats
from recon_lw.reporting.match_diff.categorizer.types.error_examples import ErrorExamples
from recon_lw.reporting.match_diff.categorizer.types import ProblemFields
from recon_lw.reporting.match_diff.categorizer.types import MatchesStats


class IErrorCategorizer(ABC):
    def __init__(self):
        self._error_stats = ErrorCategoriesStats()
        self._matches_stats = MatchesStats()
        self._problem_fields = ProblemFields()
        self._error_examples = ErrorExamples()

    def process_events(self, events: List[dict]) -> ReconErrorStatsContext:
        for event in events:
            self.process_event(event)
        return ReconErrorStatsContext(
            error_examples=self._error_examples,
            error_stats=self._error_stats,
            problem_fields=self._problem_fields,
            matches_stats=self._matches_stats
        )

    @abstractmethod
    def process_event(self, event: dict):
        pass
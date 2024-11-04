from abc import abstractmethod, ABC
from typing import List

from recon_lw.reporting.match_diff.categorizer.types import ReconErrorStatsContext
from recon_lw.reporting.match_diff.categorizer.types import ErrorCategoriesStats
from recon_lw.reporting.match_diff.categorizer.types.error_examples import ErrorExamples
from recon_lw.reporting.match_diff.categorizer.types import ProblemFields
from recon_lw.reporting.match_diff.categorizer.types import MatchesStats
from recon_lw.reporting.match_diff.categorizer.types.miss_categories_stats import MissCategoriesStats
from recon_lw.reporting.match_diff.categorizer.types.miss_examples import MissExamples


class IErrorCategorizer(ABC):
    def __init__(
            self,
            error_stats=ErrorCategoriesStats(),
            matches_stats=MatchesStats(),
            miss_stats=MissCategoriesStats(),
            problem_fields=ProblemFields(),
            error_examples=ErrorExamples(),
            miss_examples=MissExamples()
    ):
        self._error_stats = error_stats
        self._matches_stats = matches_stats
        self._miss_stats = miss_stats
        self._problem_fields = problem_fields
        self._error_examples = error_examples
        self._miss_examples = miss_examples

    def process_events(self, events: List[dict]) -> ReconErrorStatsContext:
        for event in events:
            self.process_event(event)
        return ReconErrorStatsContext(
            error_examples=self._error_examples,
            error_stats=self._error_stats,
            problem_fields=self._problem_fields,
            matches_stats=self._matches_stats,
            misses_examples=self._miss_examples,
            misses_stats=self._miss_stats,
        )

    @abstractmethod
    def process_event(self, event: dict):
        """It expects that this function will collect metadata and store it to:
             - self._error_stats
             - self._matches_stats
             - self._problem_fields
             - self._error_examples
            """
        pass

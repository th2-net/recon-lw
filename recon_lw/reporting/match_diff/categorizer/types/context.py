from dataclasses import dataclass

from recon_lw.reporting.match_diff.categorizer.types.error_categories_stats import ErrorCategoriesStats
from recon_lw.reporting.match_diff.categorizer.types.error_examples import ErrorExamples
from recon_lw.reporting.match_diff.categorizer.types.field_problems import ProblemFields
from recon_lw.reporting.match_diff.categorizer.types.match_stats import MatchesStats


@dataclass
class ReconErrorStatsContext:
    error_examples: ErrorExamples
    error_stats: ErrorCategoriesStats
    problem_fields: ProblemFields
    matches_stats: MatchesStats
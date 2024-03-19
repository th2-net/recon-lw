from collections import defaultdict
from typing import Dict


class MatchesStats:
    def __init__(self, match_categories: Dict[str, int]=None):
        if not match_categories:
            match_categories = defaultdict(int)
        self.match_categories = match_categories

    def add_match(self, recon_name: str):
        self.match_categories[recon_name] += 1

    def match_number(self, recon_name: str):
        return self.match_categories[recon_name]
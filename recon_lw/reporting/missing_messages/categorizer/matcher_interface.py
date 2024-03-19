from typing import Protocol, Dict, Tuple, Optional


class MissMatcher(Protocol):
    def __call__(self, event: dict) -> bool:
        pass

class MissCategorizer(Protocol):
    def __call__(self, recon_error: str, miss_event: Dict) -> Optional[Tuple[str, str]]:
        pass


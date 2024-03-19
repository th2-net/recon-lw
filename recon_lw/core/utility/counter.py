from dataclasses import dataclass

@dataclass
class Counters:
    match_ok: int = 0
    match_fail: int = 0
    no_right: int = 0
    no_left: int = 0
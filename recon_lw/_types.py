from typing import Dict, NewType, TypeVar

# Th2Timestamp = Dict[str, int]

# Th2Timestamp = NewType('Th2Timestamp', Dict[str, int])
Th2Timestamp = TypeVar('Th2Timestamp', bound=Dict[str, int])

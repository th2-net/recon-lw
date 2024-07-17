from typing import Optional, Callable, Protocol
from abc import ABC, abstractmethod

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from recon_lw.core.rule.base import AbstractRule


class FlushFunction(ABC):

    def __call__(self,
                 timestamp: Optional[float],
                 rule: 'AbstractRule',
                 save_events_func: Callable[[dict], None]
                 ):
        return self.flush(timestamp, rule, save_events_func)

    @abstractmethod
    def flush(self,
              timestamp: Optional[float],
              rule: 'AbstractRule',
              save_events_func: Callable[[dict], None]
              ):
        pass


class FlushFunctionProtocol(Protocol):
    def __call__(self,
                 timestamp: Optional[float],
                 rule: dict,
                 save_events_func: Callable[[dict], None]):
        pass

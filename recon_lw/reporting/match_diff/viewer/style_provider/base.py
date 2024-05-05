from abc import ABC, abstractmethod
from typing import Protocol


class ErrorExamplesStyleProvider(ABC):

    def __call__(self) -> str:
        return self.get_styles()

    @abstractmethod
    def get_styles(self) -> str:
        pass


class ErrorExamplesStyleProviderProtocol(Protocol):
    def __call__(self) -> str:
        pass

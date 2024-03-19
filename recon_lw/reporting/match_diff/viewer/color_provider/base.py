from abc import abstractmethod, ABC
from typing import Protocol


class ICategoryColorProvider(ABC):

    def __call__(self, category: str) -> str:
        return self.get_category_color(category)
    @abstractmethod
    def get_category_color(self, category: str) -> str:
        pass

class ICategoryColorProviderProtocol(Protocol):
    def __call__(self, category: str) -> str:
        pass

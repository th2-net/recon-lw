from abc import abstractmethod, ABC
from typing import Protocol

from recon_lw.reporting.match_diff.categorizer import EventCategory


class ICategoryColorProvider(ABC):
    """
    Defines Color of the error example line by provided Category.

    E.g.
    -----------------------         {
    Failed field: A  1 != 2         {   This line color!
    ------------------------        {
    Content         |   Content
    Example msg 1   |   Example msg 2

    """

    def __call__(self, category: EventCategory) -> str:
        return self.get_category_color(category)

    @abstractmethod
    def get_category_color(self, category: EventCategory) -> str:
        pass


class ICategoryColorProviderProtocol(Protocol):
    def __call__(self, category: EventCategory) -> str:
        pass

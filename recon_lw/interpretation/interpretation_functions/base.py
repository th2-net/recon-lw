from abc import ABC, abstractmethod
from recon_lw.core.type.types import InterpretationFunctionType


class InterpretationFunctionProvider(ABC):

    @abstractmethod
    def provide(self) -> InterpretationFunctionType:
        pass

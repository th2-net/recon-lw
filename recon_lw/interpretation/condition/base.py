from typing import Protocol

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message
from abc import ABC, abstractmethod


class Condition(Protocol):

    def __call__(self, message: Message, adapter: Adapter) -> bool:
        return True

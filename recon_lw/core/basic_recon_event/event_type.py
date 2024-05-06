from dataclasses import dataclass
from enum import Enum


@dataclass
class IBaseReconEventType:
    """
    Default, required parameters for Basic Recon events.

    Inherit and add more types if you need.
    """
    ROOT: str
    MATCH: str
    MISS_LEFT: str
    MISS_RIGHT: str


# BaseReconEventType is a default implementation for IBaseReconEventType.
#   It used in the Basic categorizers and other Basic classes.
BaseReconEventType = IBaseReconEventType(
    ROOT='Root',
    MATCH='Match',
    MISS_LEFT='MissLeft',
    MISS_RIGHT='MissRight',
)


class ReconEventType(Enum):
    # TODO
    #   1)
    #   I don't sure that we need `BasicRecon` prefix
    #   What's the sense to use it? We used it on the *** project because
    #   we had recon with name Basic.
    #   Actually a user can have any names for Events
    #   2)
    #   Probably it's better to change to Just class (no Enum)
    BasicReconRoot = 'BasicReconRoot'
    BasicReconMatch = 'BasicReconMatch'
    BasicReconMissLeft = 'BasicReconMissLeft'
    BasicReconMissRight = 'BasicReconMissRight'


    # The comment as reminder
    # @classmethod
    # def _missing_(cls, value: object) -> Any:
    #     return LatencyCalculationMode.SENDING_TRANSACT
    #
    # def __str__(self) -> str:
    #     if self == LatencyCalculationMode.CUSTOM:
    #         return '{} minus {}'
    #     else:
    #         return 'SendingTime minus TransactTime'
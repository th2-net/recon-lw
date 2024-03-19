from enum import Enum

class ReconType(Enum):
    BasicReconMatch = 'BasicReconMatch'
    BasicReconMissRight = 'BasicReconMissLeft'
    BasicReconMissLeft = 'BasicReconMissRight'
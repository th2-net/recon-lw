import os
from typing import List

from th2_data_services.data import Data
from th2_data_services.utils.event_utils import totals


def get_recon_events(events_directory: str):
    files = os.listdir(events_directory)
    rslt = Data([])
    for f in files:
        if not f.endswith(".pickle"):
            continue
        rslt += Data.from_cache_file(os.path.join(events_directory, f))

    return rslt
from th2_data_services.data import Data
from th2_data_services.utils.data_utils import read_all_pickle_files_from_the_folder


def get_recon_events(events_directory: str) -> Data:
    return read_all_pickle_files_from_the_folder(events_directory)

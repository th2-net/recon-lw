from recon_lw._types import Th2Timestamp


def epoch_nano_str_to_ts(s_nanos: str) -> Th2Timestamp:
    nanos = int(s_nanos)
    return {"epochSecond": nanos // 1_000_000_000, "nano": nanos % 1_000_000_000}


def ts_to_epoch_nano_str(ts: Th2Timestamp):
    return f'{ts["epochSecond"]}{str(ts["nano"]).zfill(9)}'


def time_stamp_key(ts: Th2Timestamp) -> int:
    return 1_000_000_000 * ts["epochSecond"] + ts["nano"]
    # nanos_str = str(ts["nano"]).zfill(9)
    # return str(ts["epochSecond"]) + "." + nanos_str

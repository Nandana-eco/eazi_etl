import datetime

def unix_to_ts(value):
    if value is None:
        return None
    return datetime.datetime.utcfromtimestamp(value)

def tinyint_to_bool(value):
    if value is None:
        return None
    return bool(value)
def safe_str(value):
    if value is None:
        return None
    return str(value)

def safe_int(value):
    if value is None:
        return None
    return int(value)

CONVERSION_MAPS = {
    "client_meters_field_history":{
    "meterID": safe_int,                  # int NOT NULL
    "field": safe_str,                    # char(50) NOT NULL
    "value": safe_str,                    # mediumtext
    "removed": tinyint_to_bool,           # tinyint default 0
    "modified_by": safe_str,              # char(50)
    "timestamp": unix_to_ts,              # bigint
    "extra_timestamp": safe_int,          # int unsigned
    "rowID": safe_int                     # int unsigned AUTO_INCREMENT
}
}
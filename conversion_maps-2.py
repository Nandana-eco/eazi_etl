import datetime

def unix_to_ts(value):
    if value is None:
        return None
    # If the value looks like milliseconds (13 digits), divide by 1000
    if value > 1e12:  # crude check for milliseconds
        value = value / 1000.0
    return datetime.datetime.utcfromtimestamp(value)

def tinyint_to_bool(value):
    if value is None:
        return None
    return bool(value)
def safe_int(value):
    if value in (None, '', 'DELETED', 'N/A'):
        return None
    try:
        return int(value)
    except:
        return None

CONVERSION_MAPS = {
   "tracker_main_data": {
        "rowID": int,
        "clientID": safe_int,
        "mpan": str,
        "supplier_to": str,
        "meter_value": safe_int,
        "contract_duration": safe_int,

        "row_add_date": unix_to_ts,

        "account_manager": str,
        "lead_gen_agent": str,
        "closer": str,

        "ecosave_status": str,
        "supplier_status": str,

        "supplier_csd": unix_to_ts,
        "team": str
    }
    # client_status table
    
    
 
}
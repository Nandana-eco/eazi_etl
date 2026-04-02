import datetime

def unix_to_ts(value):
    if value is None:
        return None
    return datetime.datetime.utcfromtimestamp(value)

def tinyint_to_bool(value):
    if value is None:
        return None
    return bool(value)

CONVERSION_MAPS = {
    "client_meters":  {
    "meterID": int,
    "clientID": int,

    "sold_date": unix_to_ts,
    "active": tinyint_to_bool,

    "deal_stage": str,
    "meter_status": str,
    "submit_status": str,
    "meter_status_notes": str,
    "supplier_deal_status": str,
    "supplier_deal_status_notes": str,

    "csd": unix_to_ts,
    "ced": unix_to_ts,
    "get_price_ced": unix_to_ts,
    "ced_ooc": tinyint_to_bool,

    "ced_confirmed_by": str,
    "ced_confirmed_by_date": str,
    "top_line": str,

    "measurement_class": str,
    "measurement_class_extended": str,
    "mpan": str,
    "meter_type": str,
    "energised_status": str,

    "meter_tariff_type": str,
    "show_all_tariffs": tinyint_to_bool,
    "meter_pc": int,

    "full_supplier_num": str,
    "account_number": str,
    "meter_mtc": str,
    "meter_llf": str,
    "meter_dist": str,

    "temp_mpan": str,
    "ex_master_date": str,
    "ms_number": str,

    "supplier_from": str,
    "supplier_since": unix_to_ts,
    "supplier_since_unknown": tinyint_to_bool,

    "supplier_from_s_charge": str,
    "supplier_from_unit_charge_all": str,
    "supplier_from_unit_charge_weekday": str,
    "supplier_from_unit_charge_day": str,
    "supplier_from_unit_charge_night": str,
    "supplier_from_unit_charge_weekend": str,

    "supplier_to": str,
    "s_charge": str,
    "first_payment": float,

    "s_charge_units": str,
    "quote_source": str,
    "quote_pricebook": str,
    "source_email": str,

    "capacity_charge": str,
    "capacity_kVa": str,

    "unit_charge_all": str,
    "unit_charge_weekday": str,
    "unit_charge_day": str,
    "unit_charge_night": str,
    "unit_charge_weekend": str,

    "tariff_code": str,
    "consumption": str,
    "consumption_source": str,
    "consumption_added_by": str,

    "contract_duration": str,
    "contract_uplift": int,
    "standing_charge_uplift": float,
    "all_rate_uplift": str,

    "weekday_rate_uplift": float,
    "day_rate_uplift": float,
    "night_rate_uplift": float,
    "weekend_rate_uplift": float,

    "meter_value": str,
    "monthly_budget_plan": str,
    "est_monthly_bill": str,
    "meter_saving": float,

    "ced_notes": str,

    "meter_address_same": tinyint_to_bool,
    "meter_address": str,
    "meter_postcode": str,

    "gas_region_postcode": str,
    "gas_region": str,

    "payment_method": str,
    "authorisationID": str,

    "meter_business_name_same": tinyint_to_bool,
    "meter_business_name": str,

    "billing_address_same": tinyint_to_bool,
    "billing_address": str,
    "billing_postcode": str,

    "meter_notes": str,
    "ced_source": str,

    "ecoes_get_meter_data": str,
    "seen_by_ecoes": tinyint_to_bool,
    "removed": tinyint_to_bool,

    "modified_by": str,

    "port_supplier_to": str,
    "port_supplier_to_ds": str,
    "port_supplier_from": str,
    "port_supplier_from_ds": str,

    "json_price_data": str,

    "timestamp": unix_to_ts,
    "ced_source_timestamp": unix_to_ts,

    "price_reference": str,
    "split_setting": str,

    "metering_charge": str,
    "powwr_bot_number": str,
    "meter_debt": float,

    "ssc": str,

    "ecoes_scrape_timestamp": unix_to_ts,
    "consumption_scrape_timestamp": unix_to_ts,

    "external_sold_till": unix_to_ts,
    "external_sold_instance": str,

    "ced_lock_timestamp": unix_to_ts
}

     # -------------------------------
    # client_status table
    
    
 
}
TABLES = [
    {
        "mysql_table": "client_meters_field_history",
        "pg_table": "raw.client_meters_field_history",
        "id_col": "rowID",          # numeric column for incremental load
        "batch_size": 20000
    },
]
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from conversion_maps import CONVERSION_MAPS
from config import TABLES
import datetime
from logger_config import setup_logger
import os
from dotenv import load_dotenv

load_dotenv()
logger = setup_logger()

# -----------------------------
# Connections
# -----------------------------
mysql_conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DB")
)
mysql_cursor = mysql_conn.cursor(dictionary=True)

pg_conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DB")

)
pg_cursor = pg_conn.cursor()

# -----------------------------
# Helpers
# -----------------------------
def truncate_table(pg_table):
    print(f"Truncating {pg_table}...")
    pg_cursor.execute(f"TRUNCATE TABLE {pg_table} CASCADE")
    pg_conn.commit()

def fetch_mysql_rows(table_name, batch_size=25000):
    query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT %s OFFSET %s"
    offset = 0

    while True:
        mysql_cursor.execute(query, (batch_size, offset))
        rows = mysql_cursor.fetchall()

        if not rows:
            break

        yield rows
        offset += len(rows)

def safe_convert(func, value):
    if value is None:
        return None
    try:
        return func(value)
    except Exception as e:
        print(f"Conversion error: {value} -> {e}")
        return None

def convert_rows(rows, table_name):
    conversion_map = CONVERSION_MAPS.get(table_name, {})

    converted = []
    for row in rows:
        converted_row = {
            col: safe_convert(conversion_map.get(col, lambda x: x), row[col])
            for col in row
        }
        converted.append(converted_row)

    return converted

def insert_pg(rows, pg_table):
    if not rows:
        return

    cols = rows[0].keys()
    values = [[r[col] for col in cols] for r in rows]

    query = f"""
        INSERT INTO {pg_table} ({', '.join(cols)})
        VALUES %s
    """

    execute_values(pg_cursor, query, values)
    pg_conn.commit()

# -----------------------------
# MAIN ETL LOOP
# -----------------------------
try:
    logger.info("Starting ETL...")

    for table in TABLES:
        mysql_table = table["mysql_table"]
        pg_table = table["pg_table"]

        logger.info(f"Processing {mysql_table} → {pg_table}")

        # Step 1: truncate
        truncate_table(pg_table)

        total = 0

        # Step 2: reload
        for batch in fetch_mysql_rows(mysql_table):
            converted = convert_rows(batch, mysql_table)
            insert_pg(converted, pg_table)

            total += len(batch)
            logger.info(f"{mysql_table}: {total} rows processed")

        logger.info(f"Finished {mysql_table}")

    logger.info("ETL COMPLETED SUCCESSFULLY")

except Exception as e:
    logger.exception("ETL FAILED")  # logs full traceback

finally:
    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
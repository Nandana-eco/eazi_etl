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
    logger.info(f"Truncating {pg_table}...")
    pg_cursor.execute(f"TRUNCATE TABLE {pg_table} CASCADE")
    pg_conn.commit()

def fetch_mysql_rows(table_name, batch_size=20000, id_col="rowID", last_id=None):
    while True:
        params = []
        query = f"SELECT * FROM {table_name}"

        if last_id is not None:
            query += f" WHERE {id_col} > %s"
            params.append(last_id)

        query += f" ORDER BY {id_col} ASC LIMIT %s"
        params.append(batch_size)

        mysql_cursor.execute(query, params)
        rows = mysql_cursor.fetchall()

        if not rows:
            break

        yield rows

        # 🔑 Move the cursor forward using the last row in this batch
        last_id = rows[-1][id_col]

def safe_convert(func, value):
    if value is None:
        return None
    try:
        return func(value)
    except Exception as e:
        logger.warning(f"Conversion error: {value} -> {e}")
        return None

def convert_rows(rows, table_name):
    conversion_map = CONVERSION_MAPS.get(table_name, {})
    converted = []
    for row in rows:
        converted_row = {col: safe_convert(conversion_map.get(col, lambda x: x), row[col])
                         for col in row}
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
        ON CONFLICT ({id_col}) DO NOTHING
    """
    execute_values(pg_cursor, query, values)
    pg_conn.commit()

def get_last_row_id(table_name):
    """
    Fetch the last processed rowID from raw.etl_meta.
    Returns 0 if the table has no entry (i.e., first load).
    """
    pg_cursor.execute("""
        SELECT last_row_id
        FROM raw.etl_meta
        WHERE table_name = %s
    """, (table_name,))
    result = pg_cursor.fetchone()
    if result:
        return result[0]
    return 0
def update_etl_meta(table_name, last_row_id):
    """
    Inserts or updates the last processed rowID in etl_meta.
    """
    pg_cursor.execute("""
        INSERT INTO raw.etl_meta (table_name, last_row_id, last_run)
        VALUES (%s, %s, %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_row_id = EXCLUDED.last_row_id, last_run = EXCLUDED.last_run
    """, (table_name, last_row_id, datetime.datetime.utcnow()))
    pg_conn.commit()
    logger.info(f"Updated etl_meta for {table_name}: last_row_id = {last_row_id}")

# -----------------------------
# MAIN ETL LOOP
# -----------------------------
try:
    logger.info("Starting full-load + incremental ETL...")

    for table in TABLES:
        mysql_table = table["mysql_table"]
        pg_table = table["pg_table"]
        id_col = table.get("id_col", "rowID")  # default rowID if not specified
        batch_size = table.get("batch_size", 20000)

        # Check last row ID
        last_id = get_last_row_id(mysql_table)
        if last_id is not None:
            logger.info(f"{mysql_table}: Incremental load starting after {id_col}={last_id}")
        else:
            logger.info(f"{mysql_table}: Full load (no entry in etl_meta)")

        logger.info(f"Processing {mysql_table} → {pg_table}")

        # Step 1: truncate target table
        

        total = 0
        max_id = last_id or 0

        # Step 2: fetch, convert, and insert
        
        for batch in fetch_mysql_rows(mysql_table, batch_size=batch_size, id_col=id_col,last_id=last_id):
            converted = convert_rows(batch, mysql_table)
            insert_pg(converted, pg_table)
            
            # track max rowID
            batch_max_id = max(row[id_col] for row in batch)
            if batch_max_id > max_id:
                max_id = batch_max_id
            
            total += len(batch)


            logger.info(f"{mysql_table}: {total} rows processed, max {id_col} = {max_id}")

        # Step 3: update etl_meta with the last processed rowID
        update_etl_meta(mysql_table, max_id)

        logger.info(f"Finished full load for {mysql_table}, total rows = {total}")

    logger.info("Full-load ETL COMPLETED SUCCESSFULLY")

except Exception as e:
    logger.exception("ETL FAILED")

finally:
    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    logger.info("Connections closed, ETL finished.")
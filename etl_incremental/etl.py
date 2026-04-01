import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from conversion_maps import CONVERSION_MAPS
from config import TABLES
import datetime
from logger_config import setup_logger

logger = setup_logger()

# -----------------------------
# Connections
# -----------------------------
mysql_conn = mysql.connector.connect(
    host="ecosavepay-db.mysql.database.azure.com",
    user="nandana",
    password="TROpE45!!Polo",
    database="ecosave_esp"
)
mysql_cursor = mysql_conn.cursor(dictionary=True)

pg_conn = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="admin",
    dbname="ecosave_local"
)
pg_cursor = pg_conn.cursor()

# -----------------------------
# Helpers
# -----------------------------
def truncate_table(pg_table):
    logger.info(f"Truncating {pg_table}...")
    pg_cursor.execute(f"TRUNCATE TABLE {pg_table} CASCADE")
    pg_conn.commit()

def fetch_mysql_rows(table_name, batch_size=20000, id_col=None, last_id=None):
    offset = 0
    params = []

    query = f"SELECT * FROM {table_name}"
    if id_col and last_id is not None:
        query += f" WHERE {id_col} > %s"
        params.append(last_id)

    query += " ORDER BY {0} ASC LIMIT %s OFFSET %s".format(id_col or "1")

    while True:
        mysql_cursor.execute(query, params + [batch_size, offset])
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
    """
    execute_values(pg_cursor, query, values)
    pg_conn.commit()

def update_etl_meta(table_name, last_row_id):
    pg_cursor.execute("""
        INSERT INTO etl_meta (table_name, last_row_id, last_run)
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
    logger.info("Starting full-load ETL...")

    for table in TABLES:
        mysql_table = table["mysql_table"]
        pg_table = table["pg_table"]
        id_col = table.get("id_col", "rowID")  # default rowID if not specified
        batch_size = table.get("batch_size", 20000)

        logger.info(f"Processing {mysql_table} → {pg_table}")

        # Step 1: truncate target table
        truncate_table(pg_table)

        total = 0
        max_id = 0

        # Step 2: fetch, convert, and insert
        for batch in fetch_mysql_rows(mysql_table, batch_size=batch_size, id_col=id_col):
            converted = convert_rows(batch, mysql_table)
            insert_pg(converted, pg_table)
            total += len(batch)

            # track max rowID
            batch_max_id = max(row[id_col] for row in batch)
            if batch_max_id > max_id:
                max_id = batch_max_id

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
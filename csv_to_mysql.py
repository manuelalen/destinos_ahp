import os
import re
import math
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

CSV_PATH = "distancias_malaga.csv"
TABLE_NAME = "m_destinos_distancias"
BATCH_SIZE = 1000

load_dotenv()

def mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )

def sanitize_columns(cols):
    out = []
    used = {}
    idx = 1
    for c in cols:
        name = "" if c is None else str(c)
        if name.lower() == "nan":
            name = ""
        name = name.strip()

        if not name:
            name = f"col_{idx}"
            idx += 1

        name = name.lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^a-z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        if not name:
            name = f"col_{idx}"
            idx += 1
        if name[0].isdigit():
            name = f"c_{name}"

        if name in used:
            used[name] += 1
            name = f"{name}_{used[name]}"
        else:
            used[name] = 1

        out.append(name)
    return out

def mysql_type_for_series(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "TINYINT(1)"
    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME"

    # texto
    try:
        max_len = int(s.astype(str).map(len).max())
    except Exception:
        max_len = 255

    if max_len <= 255:
        return "VARCHAR(255)"
    if max_len <= 1000:
        return "VARCHAR(1000)"
    return "TEXT"

def create_table(cur, table: str, df: pd.DataFrame):
    cols_sql = []
    for c in df.columns:
        coltype = mysql_type_for_series(df[c])
        cols_sql.append(f"`{c}` {coltype} NULL")

    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{table}` ("
        + ", ".join(cols_sql)
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )
    cur.execute(ddl)

def to_mysql_values(row):
    """Convierte NaN/NaT a None de forma segura (incluye floats)."""
    out = []
    for v in row:
        if v is None:
            out.append(None)
            continue
        # pandas NaT / numpy nan
        try:
            if isinstance(v, float) and math.isnan(v):
                out.append(None)
                continue
        except Exception:
            pass
        # pandas missing
        if pd.isna(v):
            out.append(None)
            continue
        out.append(v)
    return tuple(out)

def insert_df(cur, table: str, df: pd.DataFrame):
    # CLAVE: forzar object para que None NO vuelva a nan
    df2 = df.copy().astype(object)

    cols = list(df2.columns)
    col_sql = ", ".join([f"`{c}`" for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))
    q = f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders})"

    total = len(df2)
    for start in range(0, total, BATCH_SIZE):
        batch = df2.iloc[start:start + BATCH_SIZE]
        values = [to_mysql_values(row) for row in batch.values.tolist()]
        cur.executemany(q, values)
        print(f"[OK] Insertadas {min(start + BATCH_SIZE, total)}/{total}")

def main():
    df = pd.read_csv(CSV_PATH)

    # sanea columnas
    df.columns = sanitize_columns(df.columns)

    cnx = mysql_conn()
    try:
        cur = cnx.cursor()
        create_table(cur, TABLE_NAME, df)
        cnx.commit()

        insert_df(cur, TABLE_NAME, df)
        cnx.commit()

        print(f"[DONE] Insertado en {os.getenv('MYSQL_DATABASE')}.{TABLE_NAME}")
    finally:
        cnx.close()

if __name__ == "__main__":
    main()

import os
import json
import re
import time
from typing import Any, Dict, List, Tuple

import mysql.connector
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# ----------------------------
# Utils
# ----------------------------
def normalize_ident(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def dedupe_idents(idents: List[str]) -> List[str]:
    seen = {}
    out = []
    for x in idents:
        if x not in seen:
            seen[x] = 1
            out.append(x)
        else:
            seen[x] += 1
            out.append(f"{x}_{seen[x]}")
    return out


def map_mysql_to_pg(data_type: str, column_type: str) -> str:
    dt = (data_type or "").lower()
    ct = (column_type or "").lower()

    if dt in ("int", "integer", "mediumint"):
        return "integer"
    if dt == "bigint":
        return "bigint"
    if dt == "smallint":
        return "smallint"
    if dt == "tinyint":
        return "boolean" if "tinyint(1)" in ct else "smallint"

    if dt in ("decimal", "numeric"):
        m = re.search(r"\((\d+),(\d+)\)", ct)
        return f"numeric({m.group(1)},{m.group(2)})" if m else "numeric"

    if dt == "float":
        return "real"
    if dt == "double":
        return "double precision"

    if dt in ("varchar", "char"):
        m = re.search(r"\((\d+)\)", ct)
        return f"varchar({m.group(1)})" if m else "text"

    if dt in ("text", "mediumtext", "longtext"):
        return "text"

    if dt == "datetime":
        return "timestamp"
    if dt == "timestamp":
        return "timestamptz"
    if dt == "date":
        return "date"
    if dt == "time":
        return "time"

    if dt == "json":
        return "jsonb"

    if dt in ("blob", "mediumblob", "longblob", "binary", "varbinary"):
        return "bytea"

    return "text"


# ----------------------------
# Connections
# ----------------------------
def mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )


def supa_pg_conn():
    dsn = os.getenv("SUPABASE_PG_DSN", "").strip()
    if not dsn:
        raise SystemExit("Falta SUPABASE_PG_DSN en el .env")
    return psycopg2.connect(dsn, connect_timeout=20)


# ----------------------------
# Metadata (MySQL)
# ----------------------------
def parse_json_field(s: str) -> Dict[str, Any]:
    return json.loads(s)


def load_all_metadata(conn) -> List[Dict[str, Any]]:
    q = """
        SELECT INGESTION_NAME, ACTIVE, SOURCE, SOURCE_TYPE, TARGET, TARGET_TYPE
        FROM M_METADATA
        WHERE ACTIVE=1
        ORDER BY INGESTION_NAME
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows or []


# ----------------------------
# MySQL introspection
# ----------------------------
def mysql_columns(conn, db: str, table: str) -> List[Dict[str, Any]]:
    q = """
    SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
    ORDER BY ORDINAL_POSITION
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(q, (db, table))
    cols = cur.fetchall()
    cur.close()
    if not cols:
        raise RuntimeError(f"No encontré columnas para {db}.{table}")
    return cols


def mysql_pk(conn, db: str, table: str) -> List[str]:
    q = """
    SELECT k.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS t
    JOIN information_schema.KEY_COLUMN_USAGE k
      ON t.CONSTRAINT_NAME=k.CONSTRAINT_NAME
     AND t.TABLE_SCHEMA=k.TABLE_SCHEMA
     AND t.TABLE_NAME=k.TABLE_NAME
    WHERE t.TABLE_SCHEMA=%s AND t.TABLE_NAME=%s AND t.CONSTRAINT_TYPE='PRIMARY KEY'
    ORDER BY k.ORDINAL_POSITION
    """
    cur = conn.cursor()
    cur.execute(q, (db, table))
    pk = [r[0] for r in cur.fetchall()]
    cur.close()
    return pk


# ----------------------------
# Supabase DDL + load
# ----------------------------
def ensure_schema_and_table(
    pg,
    schema: str,
    table: str,
    mysql_cols: List[Dict[str, Any]],
    mysql_pk_cols: List[str],
) -> Tuple[str, str, List[str], List[str]]:
    schema_norm = normalize_ident(schema)
    table_norm = normalize_ident(table)

    mysql_col_names = [c["COLUMN_NAME"] for c in mysql_cols]
    pg_col_names = dedupe_idents([normalize_ident(x) for x in mysql_col_names])

    # PK normalizada
    pk_norm = []
    if mysql_pk_cols:
        name_map = dict(zip(mysql_col_names, pg_col_names))
        pk_norm = [name_map[x] for x in mysql_pk_cols if x in name_map]

    col_defs_sql = []
    for c, pg_name in zip(mysql_cols, pg_col_names):
        pg_type = map_mysql_to_pg(c["DATA_TYPE"], c["COLUMN_TYPE"])
        nullable = (c["IS_NULLABLE"] == "YES")

        default = c["COLUMN_DEFAULT"]
        default_sql = sql.SQL("")
        if default is not None:
            if pg_type == "boolean":
                if str(default) in ("0", "false", "FALSE"):
                    default_sql = sql.SQL(" DEFAULT false")
                elif str(default) in ("1", "true", "TRUE"):
                    default_sql = sql.SQL(" DEFAULT true")
            elif pg_type.startswith(("integer", "bigint", "smallint", "numeric", "real", "double")):
                if re.fullmatch(r"[-+]?\d+(\.\d+)?", str(default)):
                    default_sql = sql.SQL(" DEFAULT ") + sql.SQL(str(default))
            elif pg_type == "text" or pg_type.startswith("varchar"):
                default_sql = sql.SQL(" DEFAULT ") + sql.Literal(str(default))

        null_sql = sql.SQL("") if nullable else sql.SQL(" NOT NULL")

        col_defs_sql.append(
            sql.SQL("{} {}{}{}").format(
                sql.Identifier(pg_name),
                sql.SQL(pg_type),
                default_sql,
                null_sql,
            )
        )

    pk_sql = sql.SQL("")
    if pk_norm:
        pk_sql = sql.SQL(", PRIMARY KEY ({})").format(
            sql.SQL(", ").join(sql.Identifier(x) for x in pk_norm)
        )

    ddl = (
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_norm))
        + sql.SQL(" CREATE TABLE IF NOT EXISTS {}.{} (").format(
            sql.Identifier(schema_norm), sql.Identifier(table_norm)
        )
        + sql.SQL(", ").join(col_defs_sql)
        + pk_sql
        + sql.SQL(");")
    )

    with pg.cursor() as cur:
        cur.execute(ddl)
    pg.commit()

    return schema_norm, table_norm, pg_col_names, mysql_col_names


def maybe_truncate(pg, schema_norm: str, table_norm: str, mode: str):
    if mode == "replace":
        with pg.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{};").format(
                    sql.Identifier(schema_norm), sql.Identifier(table_norm)
                )
            )
        pg.commit()


def load_data(
    my,
    pg,
    src_db: str,
    src_table: str,
    schema_norm: str,
    table_norm: str,
    pg_cols: List[str],
    mysql_cols: List[str],
    batch_size: int,
):
    select_sql = "SELECT " + ", ".join(f"`{c}`" for c in mysql_cols) + f" FROM `{src_db}`.`{src_table}`"
    mcur = my.cursor()
    mcur.execute(select_sql)

    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
        sql.Identifier(schema_norm),
        sql.Identifier(table_norm),
        sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
    )

    total = 0
    with pg.cursor() as pcur:
        while True:
            rows = mcur.fetchmany(batch_size)
            if not rows:
                break
            execute_values(pcur, insert_stmt, rows, page_size=len(rows))
            pg.commit()
            total += len(rows)
    mcur.close()
    return total


def main():
    load_dotenv()

    for k in ["MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE", "SUPABASE_PG_DSN"]:
        if not os.getenv(k):
            raise SystemExit(f"Falta {k} en el .env")

    # modo y defaults
    load_mode = os.getenv("LOAD_MODE", "append").strip().lower()  # append|replace
    batch_size = int(os.getenv("BATCH_SIZE", "500"))

    # schema destino:
    # - si quieres FORZAR siempre prd_ahp, deja SUPABASE_SCHEMA
    # - si quieres usar target.database como schema, pon USE_TARGET_SCHEMA=1
    fixed_schema = os.getenv("SUPABASE_SCHEMA", "prd_ahp").strip()
    use_target_schema = os.getenv("USE_TARGET_SCHEMA", "0").strip() == "1"

    my = mysql_conn()
    pg = supa_pg_conn()
    try:
        metas = load_all_metadata(my)
        if not metas:
            print("[INFO] No hay filas ACTIVE=1 en M_METADATA.")
            return

        for m in metas:
            name = m["INGESTION_NAME"]
            if m["SOURCE_TYPE"] != "table" or m["TARGET_TYPE"] != "table":
                print(f"[SKIP] {name} (solo soporta table->table)")
                continue

            source = parse_json_field(m["SOURCE"])
            target = parse_json_field(m["TARGET"])

            src_db = source["database"]
            src_table = source["table"]

            tgt_schema = normalize_ident(target.get("database", fixed_schema)) if use_target_schema else fixed_schema
            tgt_table = target["table"]

            print(f"\n[INFO] Ingestión: {name}")
            print(f"[INFO] Origen: {src_db}.{src_table}")
            print(f"[INFO] Destino: {tgt_schema}.{tgt_table}")

            cols = mysql_columns(my, src_db, src_table)
            pk = mysql_pk(my, src_db, src_table)

            schema_norm, table_norm, pg_cols, mysql_cols = ensure_schema_and_table(
                pg, tgt_schema, tgt_table, cols, pk
            )

            maybe_truncate(pg, schema_norm, table_norm, load_mode)

            total = load_data(
                my, pg, src_db, src_table, schema_norm, table_norm, pg_cols, mysql_cols, batch_size
            )
            print(f"[DONE] {name}: insertadas {total} filas")

    finally:
        pg.close()
        my.close()


if __name__ == "__main__":
    main()

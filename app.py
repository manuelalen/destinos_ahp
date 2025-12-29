import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()  # solo para local

def get_secret(key: str, default: str = "") -> str:
    if key in st.secrets:
        return str(st.secrets[key])
    return os.getenv(key, default)

DSN = get_secret("SUPABASE_PG_DSN").strip()
SCHEMA = get_secret("SUPABASE_SCHEMA", "prd_ahp").strip()
TABLE = get_secret("SUPABASE_TABLE", "vw_destinos").strip()
COL_UO = get_secret("COL_U_ORGANICA", "u_organica").strip()
COL_AREA = get_secret("COL_AREA", "area").strip()
COL_DISTANCIA = get_secret("COL_DISTANCIA", "dist_km_recta").strip()


if not DSN:
    st.error("Falta SUPABASE_PG_DSN en el .env")
    st.stop()

st.set_page_config(page_title="AHP · Destinos", layout="wide")
st.title("📋 Destinos (Supabase)")

@st.cache_resource
def get_conn():
    conn = psycopg2.connect(DSN, connect_timeout=15)
    conn.autocommit = True
    return conn

def table_ident(schema: str, table: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

def fetch_df(q: sql.SQL, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def fetch_one(q: sql.SQL, params: Tuple[Any, ...] = ()) -> Any:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(q, params)
        row = cur.fetchone()
    return row[0] if row else None

def fetch_columns(schema: str, table: str) -> List[Dict[str, Any]]:
    q = sql.SQL("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """)
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        rows = cur.fetchall()
    return [{"name": r[0], "data_type": r[1]} for r in rows]

def fetch_distinct_values(col: str, limit: int = 5000) -> List[str]:
    q = sql.SQL("""
        SELECT DISTINCT {c}
        FROM {t}
        WHERE {c} IS NOT NULL
        ORDER BY {c}
        LIMIT %s
    """).format(c=sql.Identifier(col), t=table_ident(SCHEMA, TABLE))
    dfv = fetch_df(q, (limit,))
    return dfv[col].astype(str).tolist() if not dfv.empty else []

def fetch_min_max(col: str) -> Tuple[Optional[float], Optional[float]]:
    q = sql.SQL("SELECT min({c})::float, max({c})::float FROM {t}").format(
        c=sql.Identifier(col),
        t=table_ident(SCHEMA, TABLE),
    )
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]

# ----------------------------
# Sidebar controls
# ----------------------------
cols_meta = fetch_columns(SCHEMA, TABLE)
if not cols_meta:
    st.error(f"No encuentro {SCHEMA}.{TABLE}. ¿Existe el schema/tabla?")
    st.stop()

col_names = [c["name"] for c in cols_meta]
meta_by_name = {c["name"]: c for c in cols_meta}

with st.sidebar:
    st.header("Filtros")

    # -------- Filtro U_Orgánica: incluir/excluir
    if COL_UO in col_names:
        st.subheader(COL_UO)
        uo_values = fetch_distinct_values(COL_UO)
        if uo_values:
            selected_uo = st.multiselect(
                "Incluye u_organica (desmarca para excluir)",
                options=uo_values,
                default=uo_values,
            )
        else:
            selected_uo = []
            st.info("No hay valores en u_organica.")
    else:
        selected_uo = None
        st.warning(f"No existe la columna '{COL_UO}' en la tabla.")

    # -------- Filtro Área: seleccionar áreas
    if COL_AREA in col_names:
        st.subheader(COL_AREA)
        area_values = fetch_distinct_values(COL_AREA)
        if area_values:
            selected_areas = st.multiselect(
                "Áreas (selecciona las que quieres)",
                options=area_values,
                default=[],
            )
        else:
            selected_areas = []
            st.info("No hay valores en area.")
    else:
        selected_areas = None
        st.warning(f"No existe la columna '{COL_AREA}' en la tabla.")

    st.divider()

    # -------- Filtro Distancia: rango (slider)
    if COL_DISTANCIA in col_names:
        st.subheader(COL_DISTANCIA)
        dmin, dmax = fetch_min_max(COL_DISTANCIA)
        if dmin is None or dmax is None:
            selected_dist_range = None
            st.info("No hay valores en distancia.")
        else:
            dmin_r = float(dmin)
            dmax_r = float(dmax)
            selected_dist_range = st.slider(
                "Distancia (km) rango",
                min_value=float(dmin_r),
                max_value=float(dmax_r),
                value=(float(dmin_r), float(dmax_r)),
                step=1.0,
            )
    else:
        selected_dist_range = None
        st.warning(f"No existe la columna '{COL_DISTANCIA}' en la tabla.")

    st.divider()

    # Paginación
    page_size = st.selectbox("Filas por página", [50, 100, 200, 500, 1000], index=1)
    page = st.number_input("Página", min_value=1, value=1, step=1)
    offset = (int(page) - 1) * int(page_size)

    # Orden
    st.divider()
    order_col = st.selectbox("Ordenar por", ["(sin ordenar)"] + col_names, index=0)
    order_dir = st.radio("Dirección", ["ASC", "DESC"], horizontal=True, index=0)

    st.divider()
    apply_btn = st.button("Aplicar filtros", type="primary")

# ----------------------------
# Build WHERE
# ----------------------------
def build_where() -> Tuple[sql.SQL, List[Any]]:
    parts: List[sql.SQL] = []
    params: List[Any] = []

    # u_organica
    if selected_uo is not None:
        if len(selected_uo) == 0:
            parts.append(sql.SQL("1=0"))
        else:
            parts.append(sql.SQL("{} = ANY(%s)").format(sql.Identifier(COL_UO)))
            params.append(selected_uo)

    # area
    if selected_areas is not None and len(selected_areas) > 0:
        parts.append(sql.SQL("{} = ANY(%s)").format(sql.Identifier(COL_AREA)))
        params.append(selected_areas)

    # distancia
    if selected_dist_range is not None:
        lo, hi = selected_dist_range
        parts.append(sql.SQL("{} BETWEEN %s AND %s").format(sql.Identifier(COL_DISTANCIA)))
        params.extend([lo, hi])

    if not parts:
        return sql.SQL(""), []
    return sql.SQL(" WHERE ") + sql.SQL(" AND ").join(parts), params

# Persistencia al clicar
if "where_sql" not in st.session_state:
    st.session_state.where_sql = sql.SQL("")
    st.session_state.params = []

if apply_btn:
    w, p = build_where()
    st.session_state.where_sql = w
    st.session_state.params = p

where_sql = st.session_state.where_sql
params = st.session_state.params

# ----------------------------
# Metrics (counts)
# ----------------------------
total_count = fetch_one(
    sql.SQL("SELECT count(*) FROM {t}").format(t=table_ident(SCHEMA, TABLE))
)
filtered_count = fetch_one(
    sql.SQL("SELECT count(*) FROM {t}").format(t=table_ident(SCHEMA, TABLE)) + where_sql,
    tuple(params),
)

c1, c2, c3 = st.columns(3)
c1.metric("Destinos (filtrados)", f"{filtered_count:,}".replace(",", "."))
c2.metric("Destinos (total)", f"{total_count:,}".replace(",", "."))
c3.metric("% sobre total", f"{(filtered_count / total_count * 100) if total_count else 0:.1f}%")

st.divider()

# ----------------------------
# Data query (paged)
# ----------------------------
q = sql.SQL("SELECT * FROM {t}").format(t=table_ident(SCHEMA, TABLE)) + where_sql

if order_col != "(sin ordenar)":
    q += sql.SQL(" ORDER BY {} {}").format(sql.Identifier(order_col), sql.SQL(order_dir))

q += sql.SQL(" LIMIT %s OFFSET %s")
df = fetch_df(q, tuple(params) + (page_size, offset))

st.subheader("Tabla")
st.caption(f"Página {page} · mostrando {len(df)} filas · offset {offset}")
st.dataframe(df, use_container_width=True, height=520)

# ----------------------------
# Visual extras
# ----------------------------
st.divider()
st.subheader("Vistazos rápidos")

v1, v2, v3 = st.columns(3)

with v1:
    if COL_UO in df.columns:
        st.markdown("**Top u_organica (página)**")
        st.bar_chart(df[COL_UO].astype(str).fillna("NULL").value_counts().head(15))
    else:
        st.info("No hay columna u_organica en esta vista.")

with v2:
    if COL_AREA in df.columns:
        st.markdown("**Top áreas (página)**")
        st.bar_chart(df[COL_AREA].astype(str).fillna("NULL").value_counts().head(15))
    else:
        st.info("No hay columna area en esta vista.")

with v3:
    # Histograma simple sobre una columna numérica de la página
    num_cols = []
    for c in cols_meta:
        if c["data_type"].lower() in {"smallint", "integer", "bigint", "numeric", "real", "double precision", "decimal"}:
            num_cols.append(c["name"])
    if num_cols:
        coln = st.selectbox("Numérico (histograma en página)", num_cols, index=0)
        series = pd.to_numeric(df[coln], errors="coerce").dropna()
        if len(series) > 0:
            binned = pd.cut(series, bins=20).value_counts().sort_index()
            st.bar_chart(binned)
        else:
            st.info("No hay valores numéricos en la página.")
    else:
        st.info("No detecto columnas numéricas.")

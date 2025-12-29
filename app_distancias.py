import os
import time
import math
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

load_dotenv()

ORIGIN_ADDRESS = os.getenv("ORIGIN_ADDRESS", "Calle Donato Jiménez, 2, Málaga, España")
ORS_API_KEY = os.getenv("ORS_API_KEY", "").strip()

st.set_page_config(page_title="Distancia a Málaga", layout="wide")
st.title("📍 Cercanía de localidades a Málaga (Calle Donato Jiménez, 2)")

# ---------- Helpers ----------
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    # Radio Tierra (km)
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

@st.cache_resource
def get_geocoder():
    geolocator = Nominatim(user_agent="ahp_distancias_streamlit")
    # 1 req/seg para respetar Nominatim
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)
    return geocode

@st.cache_data
def geocode_address(addr: str):
    geocode = get_geocoder()
    loc = geocode(addr)
    if not loc:
        return None
    return {"lat": loc.latitude, "lon": loc.longitude, "display": loc.address}

def ors_driving(origin, dest):
    """
    OpenRouteService driving-car: devuelve km y minutos.
    Requiere ORS_API_KEY en .env
    """
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}
    body = {"coordinates": [[origin["lon"], origin["lat"]], [dest["lon"], dest["lat"]]]}
    r = requests.post(url, json=body, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    seg = data["features"][0]["properties"]["segments"][0]
    km = seg["distance"] / 1000.0
    minutes = seg["duration"] / 60.0
    return km, minutes

def read_any(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(file)
    raise ValueError("Formato no soportado. Sube CSV o Excel.")

# ---------- UI ----------
with st.sidebar:
    st.header("Entrada")
    st.write("Sube un CSV/Excel con una columna **localidad**.")
    uploaded = st.file_uploader("Archivo", type=["csv", "xlsx", "xls"])

    st.divider()
    st.header("Opciones")
    default_col = "localidad"
    col_localidad = st.text_input("Nombre columna de localidad", value=default_col)
    extra_context = st.text_input("Contexto extra (opcional)", value="España")
    do_driving = st.checkbox("Calcular en coche (OpenRouteService)", value=bool(ORS_API_KEY))
    st.caption("Si no pones ORS_API_KEY, solo se calcula línea recta.")

    st.divider()
    st.header("Referencia")
    origin_addr = st.text_input("Dirección origen", value=ORIGIN_ADDRESS)

if not uploaded:
    st.info("Sube un archivo para empezar.")
    st.stop()

df = read_any(uploaded)
if col_localidad not in df.columns:
    st.error(f"No encuentro la columna '{col_localidad}'. Columnas disponibles: {list(df.columns)}")
    st.stop()

st.write("### Datos cargados")
st.dataframe(df.head(30), use_container_width=True)

# Geocode origin
st.write("### Geocodificando origen…")
origin = geocode_address(origin_addr)
if not origin:
    st.error("No pude geocodificar la dirección de origen. Revisa el texto.")
    st.stop()
st.success(f"Origen OK: {origin['display']} ({origin['lat']:.6f}, {origin['lon']:.6f})")

# Procesar localidades
st.write("### Calculando distancias…")

results = []
progress = st.progress(0)
n = len(df)

for i, row in df.iterrows():
    loc_txt = str(row[col_localidad]).strip()
    query = loc_txt
    if extra_context.strip():
        query = f"{loc_txt}, {extra_context.strip()}"

    g = geocode_address(query)
    if not g:
        results.append({
            "localidad": loc_txt,
            "geo_ok": False,
            "lat": None, "lon": None,
            "dist_recta_km": None,
            "dist_coche_km": None,
            "tiempo_coche_min": None
        })
        progress.progress(min((i+1)/n, 1.0))
        continue

    dist_recta = haversine_km(origin["lat"], origin["lon"], g["lat"], g["lon"])

    dist_drive_km = None
    drive_min = None
    if do_driving and ORS_API_KEY:
        try:
            dist_drive_km, drive_min = ors_driving(origin, g)
            # ORS tiene límites: pequeño delay
            time.sleep(0.2)
        except Exception:
            dist_drive_km, drive_min = None, None

    results.append({
        "localidad": loc_txt,
        "geo_ok": True,
        "lat": g["lat"], "lon": g["lon"],
        "dist_recta_km": round(dist_recta, 2),
        "dist_coche_km": round(dist_drive_km, 2) if dist_drive_km is not None else None,
        "tiempo_coche_min": round(drive_min, 1) if drive_min is not None else None
    })

    progress.progress(min((i+1)/n, 1.0))

out = pd.DataFrame(results)

# Unir con el df original (manteniendo orden)
df_out = df.copy()
df_out["_localidad_key"] = df_out[col_localidad].astype(str).str.strip()
out["_localidad_key"] = out["localidad"].astype(str).str.strip()

df_out = df_out.merge(out.drop(columns=["localidad"]), on="_localidad_key", how="left")
df_out = df_out.drop(columns=["_localidad_key"])

st.write("## Resultado")
st.metric("Localidades (total)", len(df_out))
st.metric("Geocodificadas OK", int(df_out["geo_ok"].fillna(False).sum()))
st.dataframe(df_out, use_container_width=True, height=520)

# Visuales rápidos
st.write("## Vistazos rápidos")
c1, c2 = st.columns(2)
with c1:
    st.write("### Distancia en línea recta (km)")
    if "dist_recta_km" in df_out:
        st.bar_chart(df_out["dist_recta_km"].dropna().sort_values().head(30))
with c2:
    st.write("### Top 30 más cercanas (recta)")
    if "dist_recta_km" in df_out:
        top = df_out[[col_localidad, "dist_recta_km"]].dropna().sort_values("dist_recta_km").head(30)
        st.dataframe(top, use_container_width=True, height=520)

# Descargas
st.write("## Descargar")
csv_bytes = df_out.to_csv(index=False).encode("utf-8")
st.download_button("Descargar CSV", data=csv_bytes, file_name="distancias_malaga.csv", mime="text/csv")

xlsx_buf = None
try:
    import io
    xlsx_buf = io.BytesIO()
    df_out.to_excel(xlsx_buf, index=False)
    st.download_button("Descargar Excel", data=xlsx_buf.getvalue(), file_name="distancias_malaga.xlsx")
except Exception:
    st.caption("No pude generar Excel (revisa openpyxl).")

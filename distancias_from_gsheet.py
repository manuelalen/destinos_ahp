import os
import json
import math
import time
from typing import Dict, Any, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def sheet_export_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    r = requests.get(url, timeout=30)
    # Si está privado, Google suele responder 403 o HTML de login
    if r.status_code != 200 or "text/html" in r.headers.get("Content-Type", ""):
        raise RuntimeError(
            f"No pude descargar el sheet como CSV (status={r.status_code}). "
            "Asegúrate de que el Google Sheet esté compartido como 'Cualquiera con el enlace' "
            "o usa la alternativa con credenciales."
        )
    from io import StringIO
    return pd.read_csv(StringIO(r.text))


def load_cache(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(path: str, cache: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def make_geocoder():
    geolocator = Nominatim(user_agent="ahp_distancias_script")
    # Respeta Nominatim: mínimo 1 segundo entre peticiones
    return RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)


def geocode_cached(query: str, geocode_fn, cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    key = query.strip().lower()
    if key in cache:
        return cache[key]

    loc = geocode_fn(query)
    if not loc:
        cache[key] = None
        return None

    data = {"lat": loc.latitude, "lon": loc.longitude, "display": loc.address}
    cache[key] = data
    return data


def main():
    load_dotenv()

    sheet_id = os.getenv("SHEET_ID", "").strip()
    gid = os.getenv("SHEET_GID", "0").strip()
    localidad_col = os.getenv("LOCALIDAD_COL", "LOCALIDAD").strip()

    origin_address = os.getenv("ORIGIN_ADDRESS", "Calle Donato Jiménez, 2, Málaga, España").strip()
    country_suffix = os.getenv("COUNTRY_SUFFIX", "España").strip()

    out_csv = os.getenv("OUT_CSV", "distancias_malaga.csv").strip()
    cache_file = os.getenv("CACHE_FILE", "geocode_cache.json").strip()

    if not sheet_id:
        raise SystemExit("Falta SHEET_ID en el .env")

    # 1) Descargar sheet
    df = sheet_export_csv(sheet_id, gid)
    if localidad_col not in df.columns:
        raise SystemExit(
            f"No encuentro la columna '{localidad_col}'. Columnas disponibles: {list(df.columns)}"
        )

    # 2) Preparar lista de localidades (únicas, limpias)
    localidades = (
        df[localidad_col]
        .astype(str)
        .str.strip()
        .replace({"nan": ""})
    )
    localidades = [x for x in localidades.tolist() if x]

    # 3) Geocoder + cache
    cache = load_cache(cache_file)
    geocode_fn = make_geocoder()

    # 4) Geocodificar origen
    origin = geocode_cached(origin_address, geocode_fn, cache)
    if not origin:
        raise SystemExit(f"No pude geocodificar el origen: {origin_address}")

    # 5) Calcular distancias por localidad
    rows = []
    total = len(localidades)
    for i, loc in enumerate(localidades, start=1):
        query = f"{loc}, {country_suffix}" if country_suffix else loc
        g = geocode_cached(query, geocode_fn, cache)

        if not g:
            rows.append({
                "Localidad": loc,
                "geo_ok": False,
                "lat": None,
                "lon": None,
                "dist_km_recta": None
            })
        else:
            dkm = haversine_km(origin["lat"], origin["lon"], g["lat"], g["lon"])
            rows.append({
                "Localidad": loc,
                "geo_ok": True,
                "lat": g["lat"],
                "lon": g["lon"],
                "dist_km_recta": round(dkm, 2)
            })

        # Guarda cache cada 25 para no perder trabajo
        if i % 25 == 0:
            save_cache(cache_file, cache)
            print(f"[INFO] {i}/{total} procesadas...")

    save_cache(cache_file, cache)

    # 6) Unir resultado al DF original (por localidad)
    out = pd.DataFrame(rows)
    df_out = df.merge(out, how="left", left_on=localidad_col, right_on="Localidad")
    # Si no quieres duplicar la columna:
    # df_out = df_out.drop(columns=["Localidad_y"]).rename(columns={"Localidad_x": "Localidad"})
    df_out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] CSV generado: {out_csv}")


if __name__ == "__main__":
    main()

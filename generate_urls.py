import os, re
import pandas as pd
from unidecode import unidecode
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GID = int(os.getenv("GID", "0"))
SA_JSON_PATH = os.getenv("GOOGLE_SA_JSON", "service_account.json").strip()
MUNICIPIO_COL = os.getenv("MUNICIPIO_COL", "LOCALIDAD").strip()

# Filtros que quieres codificar en la URL (ajusta)
MAX_PRICE = int(os.getenv("MAX_PRICE", "650"))
LONG_TERM = os.getenv("LONG_TERM", "1") == "1"  # 1/0
MIN_M2 = os.getenv("MIN_M2", "")  # "" o número
MAX_M2 = os.getenv("MAX_M2", "")
BEDROOMS = os.getenv("BEDROOMS", "")  # "" o 1/2/3/4+
GOOD_CONDITION = os.getenv("GOOD_CONDITION", "0") == "1"

BASE = "https://www.idealista.com/es/geo/alquiler-viviendas"

def slugify(text: str) -> str:
    t = unidecode(str(text)).lower().strip()
    t = re.sub(r"[^\w\s-]", "", t)
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"-{2,}", "-", t)
    return t.strip("-")

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def gid_to_sheet_title(service, spreadsheet_id: str, gid: int) -> str:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if int(props.get("sheetId", -1)) == int(gid):
            return props["title"]
    raise ValueError(f"No existe pestaña con gid/sheetId={gid}")

def read_sheet_as_df(service, spreadsheet_id: str, sheet_title: str) -> pd.DataFrame:
    resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_title).execute()
    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    max_len = len(header)
    rows = [r + [""] * (max_len - len(r)) for r in rows]
    return pd.DataFrame(rows, columns=header)

def build_url(municipio: str) -> str:
    parts = [BASE, slugify(municipio)]

    if LONG_TERM:
        parts.append("con-alquiler-de-larga-temporada")

    if MAX_PRICE:
        parts.append(f"con-precio-hasta_{MAX_PRICE}")

    if MIN_M2:
        parts.append(f"con-metros-cuadrados-mas-de_{int(MIN_M2)}")
    if MAX_M2:
        parts.append(f"con-metros-cuadrados-menos-de_{int(MAX_M2)}")

    if BEDROOMS:
        b = BEDROOMS.strip()
        if b == "1":
            parts.append("con-un-dormitorio")
        elif b == "2":
            parts.append("con-dos-dormitorios")
        elif b == "3":
            parts.append("con-de-tres-dormitorios")
        else:
            parts.append("con-cuatro-dormitorios-o-mas")

    if GOOD_CONDITION:
        parts.append("con-buen-estado")

    return "/".join(parts) + "/"

def main():
    if not SHEET_ID:
        raise SystemExit("Falta SHEET_ID en .env")
    if not os.path.exists(SA_JSON_PATH):
        raise SystemExit(f"No encuentro {SA_JSON_PATH}")

    svc = get_sheets_service()
    title = gid_to_sheet_title(svc, SHEET_ID, GID)
    df = read_sheet_as_df(svc, SHEET_ID, title)

    if MUNICIPIO_COL not in df.columns:
        raise SystemExit(f"No existe columna {MUNICIPIO_COL}. Columnas: {list(df.columns)}")

    municipios = (
        df[MUNICIPIO_COL].astype(str).str.strip()
        .replace({"nan": ""})
    )
    municipios = [m for m in municipios.unique().tolist() if m]

    out = []
    for m in municipios:
        out.append({"municipio": m, "idealista_url": build_url(m)})

    pd.DataFrame(out).to_csv("urls.csv", index=False, encoding="utf-8")
    print("OK -> urls.csv")

if __name__ == "__main__":
    main()

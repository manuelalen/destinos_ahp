import re
import pandas as pd
import requests
import streamlit as st

SHEET_URL = "https://docs.google.com/spreadsheets/d/1jA5XefBVg7D-pFvBN0RoVx35iegU6fc5XL6BDOh3D14/edit?gid=0#gid=0"


def sheet_edit_url_to_csv_export(url: str) -> str:
    """
    Convierte:
      https://docs.google.com/spreadsheets/d/<ID>/edit?gid=0#gid=0
    en:
      https://docs.google.com/spreadsheets/d/<ID>/export?format=csv&gid=0
    """
    m = re.search(r"/d/([^/]+)/", url)
    if not m:
        raise ValueError("No encuentro el sheet_id en la URL.")
    sheet_id = m.group(1)

    gid = "0"
    m2 = re.search(r"gid=([0-9]+)", url)
    if m2:
        gid = m2.group(1)

    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


@st.cache_data(show_spinner=False)
def load_google_sheet_as_df(sheet_url: str) -> pd.DataFrame:
    csv_url = sheet_edit_url_to_csv_export(sheet_url)
    r = requests.get(csv_url, timeout=30)
    r.raise_for_status()

    # pandas puede leer desde texto CSV directamente
    from io import StringIO
    return pd.read_csv(StringIO(r.text))


def main():
    st.set_page_config(page_title="Leer Google Sheet", layout="wide")
    st.title("Lectura de Google Sheets como 'Excel'")

    st.write("URL fija:")
    st.code(SHEET_URL)

    try:
        df = load_google_sheet_as_df(SHEET_URL)
    except requests.HTTPError as e:
        st.error(
            "Error HTTP al leer el Sheet. "
            "Normalmente es porque NO es público o requiere login.\n\n"
            f"Detalle: {e}"
        )
        st.stop()
    except Exception as e:
        st.error(f"No pude leer el Sheet: {e}")
        st.stop()

    st.success(f"OK: {len(df)} filas, {len(df.columns)} columnas")
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()

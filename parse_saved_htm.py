import os, re, glob
import pandas as pd
from bs4 import BeautifulSoup

HTML_DIR = "saved_html"
M2_OBJ = 70
LOW = 600
HIGH = 650

PRICE_RE = re.compile(r"(Average price|Precio medio)\s*:\s*([\d\.,]+)\s*(eur|€)\s*/\s*m²", re.IGNORECASE)

def parse_price_m2(path: str):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    m = PRICE_RE.search(text)
    if not m:
        return None
    raw = m.group(2).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def classify(rent):
    if rent is None or (isinstance(rent, float) and pd.isna(rent)):
        return "SIN DATO"
    if rent <= LOW:
        return "BARATO"
    if rent <= HIGH:
        return "MEDIO"
    return "CARO"

def main():
    files = glob.glob(os.path.join(HTML_DIR, "*.html")) + glob.glob(os.path.join(HTML_DIR, "*.htm"))
    if not files:
        raise SystemExit(f"No hay HTML en {HTML_DIR}/")

    rows = []
    for f in files:
        precio_m2 = parse_price_m2(f)
        alquiler = (precio_m2 * M2_OBJ) if precio_m2 else None
        rows.append({
            "file": os.path.basename(f),
            "precio_m2": precio_m2,
            "m2_obj": M2_OBJ,
            "alquiler_estimado": alquiler,
            "categoria_600_650": classify(alquiler),
        })

    df = pd.DataFrame(rows).sort_values(["categoria_600_650", "alquiler_estimado"], na_position="last")
    df.to_csv("precios_extraidos.csv", index=False, encoding="utf-8")
    print("OK -> precios_extraidos.csv")

if __name__ == "__main__":
    main()

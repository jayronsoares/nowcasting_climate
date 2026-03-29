# ingest.py — ingestão via ZIP histórico INMET (sem token)
import os, requests, zipfile, io, logging
import psycopg2, pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingest.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Conexão ──────────────────────────────────────────────────────────────────
conn = psycopg2.connect(os.getenv("TIMESCALE_URL"))

# Estados alvo
ESTADOS = {"SP", "RJ", "MG", "RS"}


# ── Helpers ──────────────────────────────────────────────────────────────────
def safe_float(val):
    try:
        v = str(val).strip().replace(",", ".")
        f = float(v)
        return None if f == -9999.0 else f
    except Exception:
        return None


def extract_metadata(lines: list[str]) -> dict:
    """Extrai UF, station_id, lat, lon do cabeçalho do CSV."""
    meta = {"uf": None, "station_id": None, "lat": None, "lon": None}
    for line in lines:
        l = line.strip()
        if l.startswith("UF:"):
            meta["uf"] = l.split(";")[1].strip()
        elif l.startswith("CODIGO (WMO):"):
            meta["station_id"] = l.split(";")[1].strip()
        elif l.startswith("LATITUDE:"):
            meta["lat"] = safe_float(l.split(";")[1].strip())
        elif l.startswith("LONGITUDE:"):
            meta["lon"] = safe_float(l.split(";")[1].strip())
    return meta


def parse_csv(f, filename: str) -> pd.DataFrame | None:
    """Lê um CSV do ZIP e retorna DataFrame limpo, ou None se inválido."""
    try:
        raw  = f.read().decode("latin1").splitlines()
        meta = extract_metadata(raw[:8])

        # Ignora estados fora do escopo
        if meta["uf"] not in ESTADOS:
            return None

        # Localiza linha de cabeçalho dos dados
        header_idx = next(
            (i for i, l in enumerate(raw) if l.startswith("Data;Hora")), 8
        )
        data_lines = raw[header_idx:]
        if len(data_lines) < 2:
            log.warning("CSV sem dados: %s", filename)
            return None

        # dtype=str preserva formato original de data/hora
        # evita que "0000 UTC" seja convertido para float
        df = pd.read_csv(
            io.StringIO("\n".join(data_lines)),
            sep=";",
            dtype=str,
            on_bad_lines="skip",
        )
        df.columns = [c.strip() for c in df.columns]

        col_map = {
            "Data":                                                   "date",
            "Hora UTC":                                               "hour",
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)":                      "rain_mm",
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)":          "temp_c",
            "UMIDADE RELATIVA DO AR, HORARIA (%)":                    "humidity",
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressure",
            "VENTO, VELOCIDADE HORARIA (m/s)":                       "wind_speed",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # Verifica colunas mínimas
        if not all(c in df.columns for c in ["date", "hour", "rain_mm"]):
            log.warning("Colunas mínimas ausentes em: %s", filename)
            return None

        # Preenche colunas ausentes
        for col in ["temp_c", "humidity", "pressure", "wind_speed"]:
            if col not in df.columns:
                df[col] = None

        # ── Timestamp ────────────────────────────────────────────────────────
        # Data: "2023/01/01" → normaliza separador → "2023-01-01"
        # Hora: "0000 UTC"  → remove sufixo, zfill 4 → "0000"
        df["date_clean"] = (
            df["date"].astype(str)
            .str.replace("/", "-", regex=False)
            .str.strip()
        )
        df["hour_clean"] = (
            df["hour"].astype(str)
            .str.replace(" UTC", "", regex=False)
            .str.strip()
            .str.zfill(4)
        )
        df["ts"] = pd.to_datetime(
            df["date_clean"] + " " +
            df["hour_clean"].str[:2] + ":" + df["hour_clean"].str[2:],
            format="%Y-%m-%d %H:%M",
            errors="coerce",
            utc=True,
        )
        df = df.dropna(subset=["ts"])

        # Guard: DataFrame vazio após limpeza
        if df.empty:
            log.warning("DataFrame vazio após limpeza: %s", filename)
            return None

        df["state_code"] = meta["uf"]
        df["station_id"] = meta["station_id"]
        df["lat"]        = meta["lat"]
        df["lon"]        = meta["lon"]

        return df[["state_code", "station_id", "lat", "lon", "ts",
                   "rain_mm", "temp_c", "humidity", "pressure", "wind_speed"]]

    except Exception as e:
        log.error("Erro ao parsear %s: %s", filename, e)
        return None


def insert_df(df: pd.DataFrame) -> int:
    """Insere DataFrame no Timescale. Retorna número de registros enviados."""
    records = [
        (
            row["state_code"], row["station_id"],
            row["lat"],        row["lon"],
            None,              row["ts"],
            safe_float(row["rain_mm"]),
            safe_float(row["temp_c"]),
            safe_float(row["humidity"]),
            safe_float(row["pressure"]),
            safe_float(row["wind_speed"]),
        )
        for _, row in df.iterrows()
    ]

    if not records:
        return 0

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO weather_observations
            (state_code, station_id, lat, lon, grid_id, ts,
             rain_mm, temp_c, humidity, pressure, wind_speed)
        VALUES %s
        ON CONFLICT (station_id, ts) DO NOTHING
        """,
        records,
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(records)


# ── Ingestão por ano ──────────────────────────────────────────────────────────
def ingest_year(year: int):
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    log.info("=== Iniciando ingestão: %d ===", year)

    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
    except Exception as e:
        log.error("Falha ao baixar %d.zip: %s", year, e)
        return

    mb = len(resp.content) / 1024 / 1024
    log.info("Download OK — %.1f MB", mb)

    z    = zipfile.ZipFile(io.BytesIO(resp.content))
    csvs = [f for f in z.namelist() if f.upper().endswith(".CSV")]
    log.info("%d arquivos CSV encontrados no ZIP", len(csvs))

    total_year  = 0
    contagem_uf = {e: 0 for e in ESTADOS}
    erros       = 0

    for fname in csvs:
        try:
            with z.open(fname) as f:
                df = parse_csv(f, fname)

            if df is None:
                continue

            uf = df["state_code"].iloc[0]
            n  = insert_df(df)

            total_year      += n
            contagem_uf[uf] += n

        except Exception as e:
            erros += 1
            log.error("Erro em %s: %s", fname, e)

    log.info(
        "--- %d concluído: %d registros inseridos | erros: %d ---",
        year, total_year, erros
    )
    for uf, count in sorted(contagem_uf.items()):
        if count:
            log.info("  %s: %d registros", uf, count)


# ── Verificação final ─────────────────────────────────────────────────────────
def verificar():
    cur = conn.cursor()
    cur.execute("""
        SELECT state_code, COUNT(*) AS total, MIN(ts)::date, MAX(ts)::date
        FROM weather_observations
        GROUP BY state_code
        ORDER BY state_code
    """)
    rows = cur.fetchall()
    cur.close()

    log.info("=== RESULTADO FINAL ===")
    for row in rows:
        log.info("  %s: %d registros | %s → %s", *row)


# ── Entry point ───────────────────────────────────────────────────────────────
def run(years: list[int]):
    for year in years:
        ingest_year(year)
    verificar()
    conn.close()
    log.info("Conexão encerrada. Ingestão completa.")


if __name__ == "__main__":
    run([2022, 2023, 2024, 2025])
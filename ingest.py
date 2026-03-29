# ingest.py — ingestão via ZIP histórico INMET (sem token)
import os, io, logging, zipfile
import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
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

# ── Constantes ────────────────────────────────────────────────────────────────
ESTADOS        = {"SP", "RJ", "MG", "RS"}
INMET_SENTINEL = -9999   # valor sentinela INMET para dado ausente

NUMERIC_COLS = ["rain_mm", "temp_c", "humidity", "pressure", "wind_speed"]

COL_MAP = {
    "Data":                                                   "date",
    "Hora UTC":                                               "hour",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)":                      "rain_mm",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)":          "temp_c",
    "UMIDADE RELATIVA DO AR, HORARIA (%)":                    "humidity",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressure",
    "VENTO, VELOCIDADE HORARIA (m/s)":                       "wind_speed",
}

INSERT_SQL = """
    INSERT INTO weather_observations
        (state_code, station_id, lat, lon, grid_id, ts,
         rain_mm, temp_c, humidity, pressure, wind_speed)
    VALUES %s
    ON CONFLICT (station_id, ts) DO NOTHING
"""


# ── Helpers puros (sem side effects) ──────────────────────────────────────────
def safe_float(val) -> float | None:
    """
    Converte valor do cabeçalho CSV (lat/lon) para float.
    Trata vírgula decimal — padrão em alguns arquivos INMET.
    """
    if val in (None, ""):
        return None
    try:
        return float(str(val).replace(",", "."))
    except (ValueError, TypeError):
        return None


def extract_metadata(lines: list[str]) -> dict:
    """Extrai UF, station_id, lat, lon das primeiras linhas do cabeçalho CSV."""
    meta = {"uf": None, "station_id": None, "lat": None, "lon": None}
    for line in lines:
        l = line.strip()
        if   l.startswith("UF:"):           meta["uf"]         = l.split(";")[1].strip()
        elif l.startswith("CODIGO (WMO):"): meta["station_id"] = l.split(";")[1].strip()
        elif l.startswith("LATITUDE:"):     meta["lat"]        = safe_float(l.split(";")[1].strip())
        elif l.startswith("LONGITUDE:"):    meta["lon"]        = safe_float(l.split(";")[1].strip())
    return meta


def clean_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas numéricas — resolve o problema de NaN::numeric no banco.

    Pipeline por coluna:
    1. Troca vírgula decimal → ponto (padrão INMET em algumas estações)
    2. pd.to_numeric(errors='coerce') — converte toda variante de NaN
       (string "NaN", float nan, string vazia) para pandas NaN uniforme
    3. Substitui sentinela INMET (-9999) por NaN

    Resultado: coluna com float válido ou NaN — nunca NaN::numeric no banco.
    """
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue
        df[col] = (
            df[col].astype(str)
                   .str.replace(",", ".", regex=False)
                   .pipe(pd.to_numeric, errors="coerce")
                   .where(lambda s: s != INMET_SENTINEL)
        )
    return df


def build_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói coluna ts (UTC) a partir de 'date' e 'hour'.
    Normaliza separador de data e remove sufixo " UTC" da hora.
    Linhas com timestamp inválido são descartadas via dropna.
    """
    date_clean = (
        df["date"].astype(str)
                  .str.replace("/", "-", regex=False)
                  .str.strip()
    )
    hour_clean = (
        df["hour"].astype(str)
                  .str.replace(" UTC", "", regex=False)
                  .str.strip()
                  .str.zfill(4)
    )
    df["ts"] = pd.to_datetime(
        date_clean + " " + hour_clean.str[:2] + ":" + hour_clean.str[2:],
        format="%Y-%m-%d %H:%M",
        errors="coerce",
        utc=True,
    )
    return df.dropna(subset=["ts"])


def parse_csv(f, filename: str) -> pd.DataFrame | None:
    """
    Lê um CSV do ZIP INMET e retorna DataFrame pronto para inserção.
    Retorna None se o arquivo não pertencer aos estados alvo ou estiver vazio.

    Pipeline: bytes → metadata → raw DataFrame → rename → clean → timestamp
    """
    try:
        raw  = f.read().decode("latin1").splitlines()
        meta = extract_metadata(raw[:8])

        if meta["uf"] not in ESTADOS:
            return None

        # Localiza a linha de cabeçalho dos dados (começa com "Data;Hora")
        header_idx = next(
            (i for i, line in enumerate(raw) if line.startswith("Data;Hora")), 8
        )
        data_lines = raw[header_idx:]
        if len(data_lines) < 2:
            log.warning("CSV sem dados: %s", filename)
            return None

        # dtype=str: preserva "0000 UTC" e evita coerção prematura de datas
        df = pd.read_csv(
            io.StringIO("\n".join(data_lines)),
            sep=";",
            dtype=str,
            on_bad_lines="skip",
        )
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})

        if not all(c in df.columns for c in ["date", "hour", "rain_mm"]):
            log.warning("Colunas mínimas ausentes: %s", filename)
            return None

        # Garante presença de todas as colunas numéricas antes da limpeza
        for col in NUMERIC_COLS:
            if col not in df.columns:
                df[col] = pd.NA

        df = clean_numeric_cols(df)
        df = build_timestamp(df)

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


def df_to_records(df: pd.DataFrame):
    """
    Gerador lazy: converte DataFrame em tuplas para execute_values.

    - itertuples() é ~50x mais rápido que iterrows()
    - pd.notna() converte pandas NaN → None Python antes de enviar ao banco
    - Nenhuma lista materializada em memória: execute_values consome o gerador
      diretamente, linha a linha (lazy evaluation)
    """
    for row in df.itertuples(index=False):
        yield (
            row.state_code,
            row.station_id,
            row.lat,
            row.lon,
            None,  # grid_id — reservado para fase futura
            row.ts,
            row.rain_mm    if pd.notna(row.rain_mm)    else None,
            row.temp_c     if pd.notna(row.temp_c)     else None,
            row.humidity   if pd.notna(row.humidity)   else None,
            row.pressure   if pd.notna(row.pressure)   else None,
            row.wind_speed if pd.notna(row.wind_speed) else None,
        )


def insert_df(df: pd.DataFrame, conn) -> int:
    """
    Insere DataFrame no Timescale via gerador lazy.
    Cursor gerenciado por context manager — fechado automaticamente.
    Retorna número de registros enviados.
    """
    if df.empty:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, df_to_records(df), page_size=1000)
    conn.commit()
    return len(df)


# ── Download ──────────────────────────────────────────────────────────────────
def download_zip(year: int) -> zipfile.ZipFile | None:
    """Baixa e abre o ZIP anual do INMET. Retorna None em caso de falha."""
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        log.info("Download OK — %.1f MB", len(resp.content) / 1024 / 1024)
        return zipfile.ZipFile(io.BytesIO(resp.content))
    except Exception as e:
        log.error("Falha ao baixar %d.zip: %s", year, e)
        return None


# ── Ingestão por ano ──────────────────────────────────────────────────────────
def ingest_year(year: int, conn) -> None:
    log.info("=== Iniciando ingestão: %d ===", year)

    z = download_zip(year)
    if z is None:
        return

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
            n  = insert_df(df, conn)
            total_year      += n
            contagem_uf[uf] += n

        except Exception as e:
            erros += 1
            log.error("Erro em %s: %s", fname, e)

    log.info("--- %d concluído: %d registros | erros: %d ---",
             year, total_year, erros)
    for uf, count in sorted(contagem_uf.items()):
        if count:
            log.info("  %s: %d registros", uf, count)


# ── Verificação final ─────────────────────────────────────────────────────────
def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT state_code, COUNT(*) AS total, MIN(ts)::date, MAX(ts)::date
            FROM weather_observations
            GROUP BY state_code
            ORDER BY state_code
        """)
        rows = cur.fetchall()

    log.info("=== RESULTADO FINAL ===")
    for row in rows:
        log.info("  %s: %d registros | %s → %s", *row)


# ── Entry point ───────────────────────────────────────────────────────────────
def run(years: list[int]) -> None:
    conn = psycopg2.connect(os.getenv("TIMESCALE_URL"))
    try:
        for year in years:
            ingest_year(year, conn)
        verificar(conn)
    finally:
        conn.close()
        log.info("Conexão encerrada. Ingestão completa.")


if __name__ == "__main__":
    run([2022, 2023, 2024, 2025])
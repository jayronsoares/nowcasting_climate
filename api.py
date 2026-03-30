# api.py — FastAPI para inferência de risco de chuva extrema
import os, pickle, logging
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Conexão ───────────────────────────────────────────────────────────────────
engine = create_engine(os.getenv("TIMESCALE_URL").replace("postgres://", "postgresql://", 1))

# ── Bundle — fonte de verdade para model, threshold e FEATURES ────────────────
# FEATURES nunca é redefinido aqui: vem do bundle gerado pelo train.py.
# Isso garante que treino e inferência usam exatamente a mesma lista,
# na mesma ordem — condição necessária para previsões corretas do LightGBM.
try:
    with open("model.pkl", "rb") as f:
        _bundle   = pickle.load(f)
    model     = _bundle["model"]
    THRESHOLD = _bundle["threshold"]
    FEATURES  = _bundle["features"]
    log.info("Bundle carregado: %d features | threshold=%.3f", len(FEATURES), THRESHOLD)
except (FileNotFoundError, KeyError) as e:
    log.error("Falha ao carregar model.pkl: %s", e)
    raise RuntimeError(
        "model.pkl ausente ou em formato antigo (sem bundle). "
        "Execute train.py para gerar um bundle atualizado."
    ) from e


# ── Helpers ───────────────────────────────────────────────────────────────────
def add_cyclic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deriva features cíclicas de hora e mês a partir de hour_ts.

    Deve ser idêntico ao add_cyclic_features() em train.py:
    mesmo período, mesma função trigonométrica, mesma coluna de origem.
    Qualquer divergência aqui introduz data leakage implícito na inferência.
    """
    ts = pd.to_datetime(df["hour_ts"], utc=True)
    df["hour_sin"]  = np.sin(2 * np.pi * ts.dt.hour  / 24)
    df["hour_cos"]  = np.cos(2 * np.pi * ts.dt.hour  / 24)
    df["month_sin"] = np.sin(2 * np.pi * ts.dt.month / 12)
    df["month_cos"] = np.cos(2 * np.pi * ts.dt.month / 12)
    return df


def risk_label(p: float, threshold: float) -> str:
    # Bandas relativas ao threshold calibrado — nunca hardcoded.
    # Garante invariante: critico ↔ burst=1, sempre.
    if p < threshold * 0.50: return "baixo"
    if p < threshold * 0.80: return "moderado"
    if p < threshold:        return "alto"
    return "critico"


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/predictions")
def predictions():
    """
    Agrega weather_features por estado e retorna probabilidade e nível de risco.

    Agregação: AVG para todas as features.
    O modelo foi treinado em linhas individuais por estação (weather_dataset).
    AVG é o único agregador consistente com essa distribuição — PERCENTILE_CONT(0.90)
    infla os inputs acima da distribuição vista no treino, causando falsos críticos.
    Migração para previsão por estação (Phase 3) eliminará essa limitação.
    """
    try:
        df = pd.read_sql(
            """
            SELECT
                state_code,
                MAX(hour_ts)      AS hour_ts,
                AVG(rain_1h)      AS rain_1h,
                AVG(rain_3h)      AS rain_3h,
                AVG(rain_6h)      AS rain_6h,
                AVG(rain_diff)    AS rain_diff,
                AVG(temp_avg)     AS temp_avg,
                AVG(humidity_avg) AS humidity_avg,
                AVG(pressure_avg) AS pressure_avg,
                AVG(wind_avg)     AS wind_avg
            FROM weather_features
            WHERE hour_ts = (SELECT MAX(hour_ts) FROM weather_features)
              AND state_code IN ('SP', 'RJ', 'MG', 'RS')
            GROUP BY state_code
            """,
            engine,
        )
    except Exception as e:
        log.error("Erro ao consultar weather_features: %s", e)
        raise HTTPException(status_code=503, detail="Falha na consulta ao banco de dados.")

    if df.empty:
        log.warning("/predictions: weather_features vazio ou sem dados recentes.")
        raise HTTPException(status_code=404, detail="Nenhum dado recente encontrado.")

    # Cyclic features are derived — they never produce NaN
    # Drop rows only on features that come from SQL
    base_features = [f for f in FEATURES if not f.endswith(("_sin", "_cos"))]
    df = df.dropna(subset=base_features)

    if df.empty:
        log.warning("/predictions: todas as linhas removidas após dropna nas features base.")
        raise HTTPException(status_code=404, detail="Dados insuficientes após limpeza de NaN.")

    # Features cíclicas derivadas do timestamp — nunca produzem NaN
    df = add_cyclic_features(df)

    # Inferência
    df["probability"] = model.predict_proba(df[FEATURES])[:, 1]
    df["burst"]       = (df["probability"] >= THRESHOLD).astype(int)
    df["risk"]        = df["probability"].apply(lambda p: risk_label(p, THRESHOLD))

    log.info(
        "/predictions: %d estados | burst detectado: %s",
        len(df),
        df[df["burst"] == 1]["state_code"].tolist() or "nenhum",
    )

    return df[["state_code", "probability", "burst", "risk"]].to_dict(orient="records")


@app.get("/health")
def health():
    """
    Retorna status da API e metadados do bundle carregado.
    Útil para confirmar que o deploy subiu com o modelo correto.
    """
    return {
        "status":     "ok",
        "threshold":  round(THRESHOLD, 4),
        "n_features": len(FEATURES),
        "features":   FEATURES,
    }

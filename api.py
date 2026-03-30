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


def risk_label(p: float) -> str:
    if p < 0.3: return "baixo"
    if p < 0.6: return "moderado"
    if p < 0.8: return "alto"
    return "critico"


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/predictions")
def predictions():
    """
    Agrega weather_features por estado e retorna probabilidade e nível de risco.

    Mudança em relação ao MVP original:
      - rain_1h / rain_3h / rain_6h / rain_avg_3h: PERCENTILE_CONT(0.90)
        em vez de AVG. O p90 representa o pior decil de estações no estado,
        tornando o sistema sensível a eventos locais sem diluir o sinal
        pela média das 40–59 estações restantes.
      - rain_diff / variáveis meteorológicas: mantêm AVG (são derivadas ou
        representam condições de fundo, não extremos pontuais).
      - hour_ts (MAX): necessário para derivar features cíclicas idênticas
        às usadas no treino.
    """
    try:
        df = pd.read_sql(
            """
            SELECT
                state_code,
                MAX(hour_ts)                                                     AS hour_ts,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY rain_1h)            AS rain_1h,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY rain_3h)            AS rain_3h,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY rain_6h)            AS rain_6h,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY rain_avg_3h)        AS rain_avg_3h,
                AVG(rain_diff)                                                   AS rain_diff,
                AVG(temp_avg)                                                    AS temp_avg,
                AVG(humidity_avg)                                                AS humidity_avg,
                AVG(pressure_avg)                                                AS pressure_avg,
                AVG(wind_avg)                                                    AS wind_avg
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

    # Features meteorológicas base — descarta linhas com qualquer NaN
    base_features = [f for f in FEATURES if f not in ("hour_sin", "hour_cos", "month_sin", "month_cos")]
    df = df.dropna(subset=base_features)

    if df.empty:
        log.warning("/predictions: todas as linhas removidas após dropna nas features base.")
        raise HTTPException(status_code=404, detail="Dados insuficientes após limpeza de NaN.")

    # Features cíclicas derivadas do timestamp — nunca produzem NaN
    df = add_cyclic_features(df)

    # Inferência
    df["probability"] = model.predict_proba(df[FEATURES])[:, 1]
    df["burst"]       = (df["probability"] >= THRESHOLD).astype(int)
    df["risk"]        = df["probability"].apply(risk_label)

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

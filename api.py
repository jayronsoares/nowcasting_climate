# api.py — FastAPI para inferência de risco de chuva extrema
import os, pickle
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],   # necessário para browsers modernos
)

# ── Conexão e modelo ──────────────────────────────────────────────────────────
# engine = create_engine(os.getenv("TIMESCALE_URL"))
engine = create_engine(os.getenv("TIMESCALE_URL").replace("postgres://", "postgresql://", 1))

with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# ── Features — deve ser idêntico ao FEATURES em train.py ─────────────────────
FEATURES = [
    "rain_1h", "rain_3h", "rain_6h", "rain_avg_3h", "rain_diff",
    "temp_avg", "humidity_avg", "pressure_avg", "wind_avg",
]


def risk_label(p: float) -> str:
    if p < 0.3: return "baixo"
    if p < 0.6: return "moderado"
    if p < 0.8: return "alto"
    return "critico"


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/predictions")
def predictions():
    """
    Lê o último hora_ts disponível no weather_features,
    agrega por estado (AVG entre estações) e retorna
    probabilidade e nível de risco para SP, RJ, MG, RS.
    """
    df = pd.read_sql(
        """
        SELECT
            state_code,
            AVG(rain_1h)      AS rain_1h,
            AVG(rain_3h)      AS rain_3h,
            AVG(rain_6h)      AS rain_6h,
            AVG(rain_avg_3h)  AS rain_avg_3h,
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

    df = df.dropna(subset=FEATURES)
    df["probability"] = model.predict_proba(df[FEATURES])[:, 1]
    df["risk"]        = df["probability"].apply(risk_label)

    return df[["state_code", "probability", "risk"]].to_dict(orient="records")


@app.get("/health")
def health():
    return {"status": "ok"}

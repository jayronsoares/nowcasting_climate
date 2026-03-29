# train.py — treino do modelo de burst detection
import os, pickle
import pandas as pd
import lightgbm as lgb
from sqlalchemy import create_engine
from sklearn.metrics import roc_auc_score, classification_report
from dotenv import load_dotenv

load_dotenv()

# ── Conexão ───────────────────────────────────────────────────────────────────
# engine = create_engine(os.getenv("TIMESCALE_URL"))
engine = create_engine(os.getenv("TIMESCALE_URL").replace("postgres://", "postgresql://", 1))

# ── Features — deve ser idêntico ao FEATURES em api.py ───────────────────────
FEATURES = [
    "rain_1h", "rain_3h", "rain_6h", "rain_avg_3h", "rain_diff",
    "temp_avg", "humidity_avg", "pressure_avg", "wind_avg",
]

# ── Carga ─────────────────────────────────────────────────────────────────────
df = pd.read_sql(
    """
    SELECT state_code, hour_ts,
           rain_1h, rain_3h, rain_6h, rain_avg_3h, rain_diff,
           temp_avg, humidity_avg, pressure_avg, wind_avg,
           burst_event
    FROM weather_dataset
    WHERE rain_1h     IS NOT NULL
      AND burst_event IS NOT NULL
    ORDER BY hour_ts
    """,
    engine,
)

# ── Split temporal — nunca shuffle em séries temporais ───────────────────────
# Treino: 2022–2023 | Validação: 2024 | Teste: 2025
train = df[df.hour_ts < "2024-01-01"]
val   = df[(df.hour_ts >= "2024-01-01") & (df.hour_ts < "2025-01-01")]
test  = df[df.hour_ts >= "2025-01-01"]

X_train, y_train = train[FEATURES], train["burst_event"]
X_val,   y_val   = val[FEATURES],   val["burst_event"]
X_test,  y_test  = test[FEATURES],  test["burst_event"]

print(f"Treino:    {len(X_train):,} registros | burst: {y_train.sum():,} ({y_train.mean()*100:.2f}%)")
print(f"Validação: {len(X_val):,}  registros | burst: {y_val.sum():,} ({y_val.mean()*100:.2f}%)")
print(f"Teste:     {len(X_test):,} registros | burst: {y_test.sum():,} ({y_test.mean()*100:.2f}%)")

# ── Modelo ────────────────────────────────────────────────────────────────────
# scale_pos_weight: razão negatives/positives
# pct_burst real confirmado: ~0.75–1.37% → ratio ~80:1
# early_stopping e log_evaluation via callbacks (LightGBM >= 4.x)
model = lgb.LGBMClassifier(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=5,
    scale_pos_weight=80,
    verbose=-1,
)

model.fit(
    X_train, y_train,
    eval_set=[(X_val, y_val)],
    callbacks=[
        lgb.early_stopping(stopping_rounds=20, verbose=False),
        lgb.log_evaluation(period=50),
    ],
)

# ── Avaliação ─────────────────────────────────────────────────────────────────
for nome, X, y in [("Validação", X_val, y_val), ("Teste", X_test, y_test)]:
    preds = model.predict_proba(X)[:, 1]
    auc   = roc_auc_score(y, preds)
    print(f"\n{nome} — AUC: {auc:.3f}")
    print(classification_report(
        y,
        (preds >= 0.5).astype(int),
        target_names=["normal", "burst"],
    ))

# ── Persistência ──────────────────────────────────────────────────────────────
with open("model.pkl", "wb") as f:
    pickle.dump(model, f)

print("\n✓ Modelo salvo: model.pkl")
print("  Próximo passo: git add model.pkl && git commit -m 'add trained model' && git push")

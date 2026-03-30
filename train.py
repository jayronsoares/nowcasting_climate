# train.py — treino do modelo de burst detection
import os, pickle, logging
import numpy as np
import pandas as pd
import lightgbm as lgb
from sqlalchemy import create_engine
from sklearn.metrics import roc_auc_score, classification_report, precision_recall_curve
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Conexão ───────────────────────────────────────────────────────────────────
engine = create_engine(os.getenv("TIMESCALE_URL").replace("postgres://", "postgresql://", 1))

# ── Features — fonte de verdade: salvas no bundle, carregadas pelo api.py ─────
# Regra: qualquer coluna aqui deve ser produzida ANTES de X_train/X_val/X_test.
# Features cíclicas (hour_*, month_*) são derivadas em Python a partir de hour_ts
# — sem alteração de schema necessária.
FEATURES = [
    # Magnitude e acumulado — janelas de tempo distintas, não redundantes
    "rain_1h", "rain_3h", "rain_6h",
    # Taxa de intensificação — sinal de EVOLUÇÃO da chuva para tempestade
    "rain_diff",
    # Condições atmosféricas de fundo
    "temp_avg", "humidity_avg", "pressure_avg", "wind_avg",
    # Sazonalidade cíclica — derivadas em Python, não requerem schema change
    "hour_sin", "hour_cos", "month_sin", "month_cos",
    # rain_avg_3h removido: AVG(rain_mm/3h) é proporcional a rain_3h=SUM(rain_mm/3h)
    # em dados horários sem lacunas — redundante confirmado por feature importance=1
]

# ── Helpers de feature engineering ───────────────────────────────────────────
def add_cyclic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deriva features cíclicas de hora e mês a partir de hour_ts.

    Encoding cíclico (sin/cos) preserva a continuidade:
      - hora 23 e hora 0 ficam próximas no espaço de features
      - mês 12 e mês 1 ficam próximos

    Deve ser idêntico ao add_cyclic_features() em api.py.
    """
    ts = pd.to_datetime(df["hour_ts"], utc=True)
    df["hour_sin"]  = np.sin(2 * np.pi * ts.dt.hour  / 24)
    df["hour_cos"]  = np.cos(2 * np.pi * ts.dt.hour  / 24)
    df["month_sin"] = np.sin(2 * np.pi * ts.dt.month / 12)
    df["month_cos"] = np.cos(2 * np.pi * ts.dt.month / 12)
    return df


def calibrate_threshold(model, X_val: pd.DataFrame, y_val: pd.Series) -> float:
    """
    Encontra o threshold que maximiza F1 no conjunto de validação.

    Por que não usar 0.5?
    Com classes desbalanceadas (burst ~1%), o modelo aprende probabilidades
    calibradas para o desbalanceamento — o ponto de corte ótimo raramente
    é 0.5. A curva PR encontra o threshold que equilibra precision/recall
    para a classe minoritária (burst).

    Retorna float entre 0 e 1.
    """
    probs = model.predict_proba(X_val)[:, 1]
    precisions, recalls, thresholds = precision_recall_curve(y_val, probs)

    # f1_scores tem len = len(thresholds), precisions/recalls têm len+1
    f1_scores = (
        2 * (precisions[:-1] * recalls[:-1])
        / (precisions[:-1] + recalls[:-1] + 1e-8)
    )
    best_idx = int(np.argmax(f1_scores))
    best_threshold = float(thresholds[best_idx])

    log.info(
        "Threshold calibrado: %.3f | Precision: %.3f | Recall: %.3f | F1: %.3f",
        best_threshold,
        precisions[best_idx],
        recalls[best_idx],
        f1_scores[best_idx],
    )
    return best_threshold


# ── Carga ─────────────────────────────────────────────────────────────────────
log.info("Carregando weather_dataset...")
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
log.info("Carga concluída: %s registros", f"{len(df):,}")

# ── Feature engineering ───────────────────────────────────────────────────────
df = add_cyclic_features(df)

# ── Split temporal — nunca shuffle em séries temporais ───────────────────────
# Treino: 2022–2023 | Validação: 2024 | Teste: 2025
train = df[df.hour_ts < "2024-01-01"]
val   = df[(df.hour_ts >= "2024-01-01") & (df.hour_ts < "2025-01-01")]
test  = df[df.hour_ts >= "2025-01-01"]

X_train, y_train = train[FEATURES], train["burst_event"]
X_val,   y_val   = val[FEATURES],   val["burst_event"]
X_test,  y_test  = test[FEATURES],  test["burst_event"]

log.info("Treino:    %s registros | burst: %s (%.2f%%)",
         f"{len(X_train):,}", f"{y_train.sum():,}", y_train.mean() * 100)
log.info("Validação: %s registros | burst: %s (%.2f%%)",
         f"{len(X_val):,}",   f"{y_val.sum():,}",   y_val.mean()   * 100)
log.info("Teste:     %s registros | burst: %s (%.2f%%)",
         f"{len(X_test):,}",  f"{y_test.sum():,}",  y_test.mean()  * 100)

# ── Modelo ────────────────────────────────────────────────────────────────────
# scale_pos_weight: razão negatives/positives
# pct_burst real confirmado: ~0.75–1.37% → ratio ~80:1
#
# eval_metric vai em fit(), não no construtor — no sklearn API do LightGBM
# o construtor ignora eval_metric silenciosamente (causa: best_iteration_ = 1).
#
# first_metric_only=True: early stopping monitora apenas auc.
# Sem isso, LightGBM monitora binary_logloss + auc na ordem interna,
# e binary_logloss para de melhorar na iteração 1 em datasets desbalanceados.
log.info("Iniciando treino LightGBM...")
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
    eval_metric="auc",
    callbacks=[
        lgb.early_stopping(stopping_rounds=30, verbose=False, first_metric_only=True),
        lgb.log_evaluation(period=50),
    ],
)
log.info("Treino concluído. Melhor iteração: %d", model.best_iteration_)

# ── Calibração de threshold ───────────────────────────────────────────────────
log.info("Calibrando threshold via curva PR na validação...")
best_threshold = calibrate_threshold(model, X_val, y_val)

# ── Avaliação ─────────────────────────────────────────────────────────────────
for nome, X, y in [("Validação", X_val, y_val), ("Teste", X_test, y_test)]:
    probs = model.predict_proba(X)[:, 1]
    auc   = roc_auc_score(y, probs)
    preds = (probs >= best_threshold).astype(int)
    log.info("%s — AUC: %.3f", nome, auc)
    print(classification_report(y, preds, target_names=["normal", "burst"]))

# ── Feature importance (top 10) ───────────────────────────────────────────────
importance = (
    pd.Series(model.feature_importances_, index=FEATURES)
    .sort_values(ascending=False)
)
log.info("Feature importance (top 10):\n%s", importance.head(10).to_string())

# ── Persistência — bundle com modelo + threshold + lista de features ──────────
# api.py carrega este bundle: nunca mais hardcode de FEATURES ou threshold lá.
bundle = {
    "model":     model,
    "threshold": best_threshold,
    "features":  FEATURES,
}
with open("model.pkl", "wb") as f:
    pickle.dump(bundle, f)

log.info("Bundle salvo: model.pkl (model + threshold=%.3f + %d features)",
         best_threshold, len(FEATURES))
log.info("Próximo passo: git add model.pkl && "
         "git commit -m 'retrain: cyclic features + calibrated threshold' && git push")
import os
import json
import math
import numpy as np
import pandas as pd
from sqlalchemy import text

import joblib
import tensorflow as tf

from src.db import get_engine

MODEL_DIR = "artifacts"
MODEL_VERSION = "v1_hetero_nll"
MODEL_PATH = os.path.join(MODEL_DIR, f"{MODEL_VERSION}.keras")
SCALER_PATH = os.path.join(MODEL_DIR, f"{MODEL_VERSION}_scaler.joblib")
FEAT_PATH = os.path.join(MODEL_DIR, f"{MODEL_VERSION}_features.json")

THRESHOLDS = [15, 20, 25, 30]

def norm_cdf(z: np.ndarray) -> np.ndarray:
    # Standard normal CDF using erf
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))

def probs_ge_k(mu, sigma, k: int):
    # continuity correction: k - 0.5
    kk = k - 0.5
    sigma = np.maximum(sigma, 1e-6)
    z = (kk - mu) / sigma
    return 1.0 - norm_cdf(z)

def main():
    engine = get_engine()

    with open(FEAT_PATH, "r") as f:
        FEATURES = json.load(f)

    df = pd.read_sql(
        """
        SELECT
          as_of_date, game_id, player_id,
          opponent_team_id, home_flag, rest_days,
          rolling_pts_5, rolling_pts_10, pts_std_10,
          rolling_min_5, rolling_min_10, min_std_10,
          last_game_pts, last_game_min
        FROM v_player_features_latest
        """,
        engine,
    )

    # Load artifacts
    scaler = joblib.load(SCALER_PATH)
    model = tf.keras.models.load_model(MODEL_PATH, compile=False)

    X = df[FEATURES].astype(float).values
    Xs = scaler.transform(X).astype(np.float32)

    mu, log_var = model(Xs, training=False)
    mu = mu.numpy().reshape(-1).astype(float)
    sigma = np.sqrt(np.exp(log_var.numpy().reshape(-1))).astype(float)

    # scoring-time clamp to keep probabilities sane
    sigma = np.clip(sigma, 1.0, 25.0)

    out = df[["as_of_date", "game_id", "player_id"]].copy()
    out["mu_pts"] = mu
    out["sigma_pts"] = sigma

    out["p15"] = probs_ge_k(mu, sigma, 15)
    out["p20"] = probs_ge_k(mu, sigma, 20)
    out["p25"] = probs_ge_k(mu, sigma, 25)
    out["p30"] = probs_ge_k(mu, sigma, 30)
    
    AVG_SIGMA = 6.7 #from the evaluation

    out["conf20"] = 100.0 * out["p20"] * (AVG_SIGMA / out["sigma_pts"])
    out["conf25"] = 100.0 * out["p25"] * (AVG_SIGMA / out["sigma_pts"])
    out["conf30"] = 100.0 * out["p30"] * (AVG_SIGMA / out["sigma_pts"])

    out["model_version"] = MODEL_VERSION

    upsert_sql = """
    INSERT INTO predictions_daily (
        as_of_date, game_id, player_id,
        mu_pts, sigma_pts, p15, p20, p25, p30,
        conf20, conf25, conf30,
        model_version
    )
    VALUES (
        :as_of_date, :game_id, :player_id,
        :mu_pts, :sigma_pts, :p15, :p20, :p25, :p30,
        :conf20, :conf25, :conf30,
        :model_version
    )
    ON CONFLICT (as_of_date, game_id, player_id) DO UPDATE SET
        mu_pts = EXCLUDED.mu_pts,
        sigma_pts = EXCLUDED.sigma_pts,
        p15 = EXCLUDED.p15,
        p20 = EXCLUDED.p20,
        p25 = EXCLUDED.p25,
        p30 = EXCLUDED.p30,
        conf20 = EXCLUDED.conf20,
        conf25 = EXCLUDED.conf25,
        conf30 = EXCLUDED.conf30,
        model_version = EXCLUDED.model_version,
        created_ts = NOW();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql), out.to_dict(orient="records"))

    print(f"Upserted {len(out)} rows into predictions_daily")

if __name__ == "__main__":
    main()

import os
import json
import numpy as np
import pandas as pd

from src.db import get_engine

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import joblib

import tensorflow as tf


# ----------------------------
# Config
# ----------------------------
MODEL_DIR = "artifacts"
MODEL_VERSION = "v1_hetero_nll"

FEATURES = [
    "home_flag",
    "rest_days",
    "rolling_pts_5",
    "rolling_pts_10",
    "pts_std_10",
    "rolling_min_5",
    "rolling_min_10",
    "min_std_10",
    "last_game_pts",
    "last_game_min",
]

TARGET = "next_points"


# ----------------------------
# Heteroscedastic Gaussian NLL
# ----------------------------
@tf.function
def gaussian_nll(y_true, mu, log_var):
    y_true = tf.cast(y_true, mu.dtype)
    log_var = tf.cast(log_var, mu.dtype)
    return 0.5 * (log_var + tf.square(y_true - mu) / tf.exp(log_var))



def build_model(d_in: int) -> tf.keras.Model:
    x_in = tf.keras.Input(shape=(d_in,), name="x")

    h = tf.keras.layers.Dense(128, activation="relu")(x_in)
    h = tf.keras.layers.Dense(64, activation="relu")(h)
    h = tf.keras.layers.Dense(64, activation="relu")(h)

    mu = tf.keras.layers.Dense(1, name="mu")(h)
    log_var = tf.keras.layers.Dense(1, name="log_var")(h)

    return tf.keras.Model(inputs=x_in, outputs=[mu, log_var])


def main():
    os.makedirs(MODEL_DIR, exist_ok=True)

    engine = get_engine()
    df = pd.read_sql("SELECT * FROM player_model_train;", engine)

    # Basic cleaning, just in case
    for c in FEATURES + [TARGET]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=FEATURES + [TARGET]).copy()

    # Time split to reduce leakage (train earlier dates, validate later dates)
    df["next_game_date"] = pd.to_datetime(df["next_game_date"])
    df = df.sort_values("next_game_date").reset_index(drop=True)

    cutoff = df["next_game_date"].quantile(0.80)
    train_df = df[df["next_game_date"] <= cutoff].copy()
    val_df = df[df["next_game_date"] > cutoff].copy()

    X_train = train_df[FEATURES].astype(np.float32).values
    y_train = train_df[TARGET].astype(np.float32).values.reshape(-1, 1)

    X_val = val_df[FEATURES].astype(np.float32).values
    y_val = val_df[TARGET].astype(np.float32).values.reshape(-1, 1)


    # Scale inputs
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train).astype(np.float32)
    X_val_s = scaler.transform(X_val).astype(np.float32)

    model = build_model(X_train_s.shape[1])
    optimizer = tf.keras.optimizers.Adam(learning_rate=1e-3)

    # Datasets
    batch_size = 256
    train_ds = tf.data.Dataset.from_tensor_slices((X_train_s, y_train)).shuffle(8192).batch(batch_size)
    val_ds = tf.data.Dataset.from_tensor_slices((X_val_s, y_val)).batch(batch_size)

    best_val = float("inf")
    patience = 6
    bad = 0

    for epoch in range(1, 61):
        tr_losses = []
        for xb, yb in train_ds:
            with tf.GradientTape() as tape:
                mu, log_var = model(xb, training=True)
                loss = tf.reduce_mean(gaussian_nll(yb, mu, log_var))
            grads = tape.gradient(loss, model.trainable_variables)
            optimizer.apply_gradients(zip(grads, model.trainable_variables))
            tr_losses.append(loss.numpy())

        va_losses = []
        for xb, yb in val_ds:
            mu, log_var = model(xb, training=False)
            loss = tf.reduce_mean(gaussian_nll(yb, mu, log_var))
            va_losses.append(loss.numpy())

        tr = float(np.mean(tr_losses))
        va = float(np.mean(va_losses))

        # Extra sanity metrics
        mu_val, log_var_val = model(X_val_s, training=False)
        mu_val = mu_val.numpy().reshape(-1)
        sigma_val = np.sqrt(np.exp(log_var_val.numpy().reshape(-1)))
        mae = float(np.mean(np.abs(y_val.reshape(-1) - mu_val)))

        print(f"epoch {epoch:02d} train_nll={tr:.4f} val_nll={va:.4f} val_mae={mae:.4f} sigma_med={np.median(sigma_val):.3f}")

        if va < best_val - 1e-4:
            best_val = va
            bad = 0
            model.save(os.path.join(MODEL_DIR, f"{MODEL_VERSION}.keras"))
        else:
            bad += 1
            if bad >= patience:
                print("early stopping")
                break

    # Save scaler and feature list
    joblib.dump(scaler, os.path.join(MODEL_DIR, f"{MODEL_VERSION}_scaler.joblib"))
    with open(os.path.join(MODEL_DIR, f"{MODEL_VERSION}_features.json"), "w") as f:
        json.dump(FEATURES, f, indent=2)

    # Quick final sanity check on a few predictions
    mu_all, log_var_all = model(scaler.transform(df[FEATURES].astype(float).values), training=False)
    mu_all = mu_all.numpy().reshape(-1)
    sigma_all = np.sqrt(np.exp(log_var_all.numpy().reshape(-1)))

    print("saved:", MODEL_VERSION)
    print("mu range:", float(np.min(mu_all)), float(np.max(mu_all)))
    print("sigma range:", float(np.min(sigma_all)), float(np.max(sigma_all)))

if __name__ == "__main__":
    main()

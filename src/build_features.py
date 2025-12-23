from datetime import date
import pandas as pd
from sqlalchemy import text
from src.db import get_engine
import numpy as np


def build_features(engine):
    # We compute features for rows that exist in player_game_stats (your labels table)
    # and write one row per (as_of_date, game_id, player_id) into player_features_daily.

    q = """
    WITH base AS (
        SELECT
            pgs.game_id,
            pgs.player_id,
            pgs.team_id,
            pgs.minutes,
            pgs.points,
            g.game_date::date AS game_date,
            g.home_team_id,
            g.away_team_id,
            CASE WHEN pgs.team_id = g.home_team_id THEN 1 ELSE 0 END AS home_flag,
            CASE WHEN pgs.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_team_id
        FROM player_game_stats pgs
        JOIN games g ON g.game_id = pgs.game_id
        WHERE g.game_date IS NOT NULL
    ),
    w AS (
        SELECT
            b.*,
            LAG(b.game_date) OVER (PARTITION BY b.player_id ORDER BY b.game_date) AS prev_game_date,
            LAG(b.points)    OVER (PARTITION BY b.player_id ORDER BY b.game_date) AS last_game_pts,
            LAG(b.minutes)   OVER (PARTITION BY b.player_id ORDER BY b.game_date) AS last_game_min,

            AVG(b.points) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)  AS rolling_pts_5,
            AVG(b.points) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS rolling_pts_10,

            STDDEV_SAMP(b.points) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS pts_std_10,

            AVG(b.minutes) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)  AS rolling_min_5,
            AVG(b.minutes) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS rolling_min_10,

            STDDEV_SAMP(b.minutes) OVER (PARTITION BY b.player_id ORDER BY b.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS min_std_10
        FROM base b
    )
    SELECT
        CURRENT_DATE AS as_of_date,
        game_id,
        player_id,
        opponent_team_id,
        home_flag,
        CASE
            WHEN prev_game_date IS NULL THEN NULL
            ELSE (game_date - prev_game_date)
        END AS rest_days,
        rolling_pts_5,
        rolling_pts_10,
        pts_std_10,
        rolling_min_5,
        rolling_min_10,
        min_std_10,
        last_game_pts,
        last_game_min
    FROM w;
    """

    df = pd.read_sql(q, engine)

    #Debug
    print("raw df shape:", df.shape)
    print(df.columns.tolist())
    print(df[["game_id","player_id","rolling_pts_10","pts_std_10","rolling_min_10"]].isna().mean())


    df["as_of_date"] = pd.Timestamp.today().date()
    df["as_of_date"] = df["as_of_date"].astype(str)  # keeps it simple for SQLAlchemy


        # Optional, drop rows that have no history (first game for a player)
    df = df.dropna(subset=["rolling_pts_10", "pts_std_10", "rolling_min_10"], how="any")

    # If no rows survive, do not execute an INSERT with no params
    if df.empty:
        print("No feature rows to upsert (df is empty after dropna).")
        return 0
    
    df["game_id"] = df["game_id"].astype(str)

    # These should be integers in the DB, but pandas may hold them as floats because of NaNs
    int_like_cols = ["player_id", "opponent_team_id", "home_flag", "rest_days"]

    for c in int_like_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Convert pandas NA/NaN to Python None so psycopg2 sends SQL NULL
    df = df.replace({pd.NA: None, np.nan: None})

    upsert_sql = """
    INSERT INTO player_features_daily (
        as_of_date, game_id, player_id, opponent_team_id, home_flag, rest_days,
        rolling_pts_5, rolling_pts_10, pts_std_10,
        rolling_min_5, rolling_min_10, min_std_10,
        last_game_pts, last_game_min
    )
    VALUES (
        CURRENT_DATE, :game_id, :player_id, :opponent_team_id, :home_flag, :rest_days,
        :rolling_pts_5, :rolling_pts_10, :pts_std_10,
        :rolling_min_5, :rolling_min_10, :min_std_10,
        :last_game_pts, :last_game_min
    )
    ON CONFLICT (as_of_date, game_id, player_id) DO UPDATE SET
        opponent_team_id = EXCLUDED.opponent_team_id,
        home_flag = EXCLUDED.home_flag,
        rest_days = EXCLUDED.rest_days,
        rolling_pts_5 = EXCLUDED.rolling_pts_5,
        rolling_pts_10 = EXCLUDED.rolling_pts_10,
        pts_std_10 = EXCLUDED.pts_std_10,
        rolling_min_5 = EXCLUDED.rolling_min_5,
        rolling_min_10 = EXCLUDED.rolling_min_10,
        min_std_10 = EXCLUDED.min_std_10,
        last_game_pts = EXCLUDED.last_game_pts,
        last_game_min = EXCLUDED.last_game_min;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql), df.to_dict(orient="records"))

    return len(df)


if __name__ == "__main__":
    engine = get_engine()
    n = build_features(engine)
    print(f"Upserted {n} feature rows into player_features_daily")

import time
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv3
from src.db import get_engine

SLEEP_SECONDS = 1.2


def get_last_7_dates():
    end = datetime.utcnow().date() - timedelta(days=1)
    start = end - timedelta(days=6)
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates



def min_to_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    if ":" in s:
        try:
            mm, ss = s.split(":", 1)
            return float(mm) + float(ss) / 60.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def ingest_games(engine, game_date):
    sb = scoreboardv2.ScoreboardV2(game_date=game_date)
    games = sb.game_header.get_data_frame()

    if games.empty:
        print(f"No games found for {game_date}")
        return []

    games_df = games[
        [
            "GAME_ID",
            "GAME_DATE_EST",
            "HOME_TEAM_ID",
            "VISITOR_TEAM_ID",
            "GAME_STATUS_TEXT",
        ]
    ].rename(
        columns={
            "GAME_ID": "game_id",
            "GAME_DATE_EST": "game_date",
            "HOME_TEAM_ID": "home_team_id",
            "VISITOR_TEAM_ID": "away_team_id",
            "GAME_STATUS_TEXT": "status",
        }
    )

    with engine.begin() as conn:
        for _, row in games_df.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO games (game_id, game_date, home_team_id, away_team_id, status)
                    VALUES (:game_id, :game_date, :home_team_id, :away_team_id, :status)
                    ON CONFLICT (game_id) DO NOTHING
                    """
                ),
                row.to_dict(),
            )

    print(f"Upserted {len(games_df)} games for {game_date}")
    return games_df["game_id"].tolist()


def ingest_boxscores(engine, game_ids):
    total_inserted = 0

    for game_id in game_ids:
        print(f"Ingesting boxscore for game {game_id}")

        try:
            bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
            players_df = bs.get_data_frames()[0]
        except Exception as e:
            print(f"ERROR fetching boxscore for {game_id}: {e}")
            continue

        if players_df.empty:
            print(f"WARNING: empty player_stats for game {game_id}")
            continue

        df = players_df[
            ["gameId", "personId", "teamId", "minutes", "points"]
        ].rename(
            columns={
                "gameId": "game_id",
                "personId": "player_id",
                "teamId": "team_id",
                "minutes": "minutes",
                "points": "points",
            }
        )

        df["minutes"] = df["minutes"].apply(min_to_float)

        inserted = 0
        with engine.begin() as conn:
            for _, row in df.iterrows():
                try:
                    conn.execute(
                        text(
                            """
                            INSERT INTO player_game_stats
                            (game_id, player_id, team_id, minutes, points)
                            VALUES
                            (:game_id, :player_id, :team_id, :minutes, :points)
                            ON CONFLICT (game_id, player_id) DO NOTHING
                            """
                        ),
                        row.to_dict(),
                    )
                    inserted += 1
                except Exception as e:
                    print(
                        f"ERROR inserting row for game {game_id}, player {row['player_id']}: {e}"
                    )

        total_inserted += inserted
        print(f"Inserted {inserted} player rows for game {game_id}")
        time.sleep(SLEEP_SECONDS)

    print(f"Total player rows inserted: {total_inserted}")


if __name__ == "__main__":
    engine = get_engine()
    date = get_last_7_dates()
    game_ids = ingest_games(engine, date)
    ingest_boxscores(engine, game_ids)

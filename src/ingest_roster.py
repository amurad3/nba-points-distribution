from nba_api.stats.static import teams, players
import pandas as pd
from sqlalchemy import text
from src.db import get_engine


def ingest_teams(engine):
    nba_teams = teams.get_teams()
    df = pd.DataFrame(nba_teams)

    df = df.rename(columns={
        "id": "team_id",
        "abbreviation": "team_abbr",
        "full_name": "team_name"
    })[["team_id", "team_abbr", "team_name"]]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE teams CASCADE"))
        df.to_sql("teams", conn, if_exists="append", index=False)

    print(f"Inserted {len(df)} teams")


def ingest_players(engine):
    nba_players = players.get_players()
    df = pd.DataFrame(nba_players)

    df = df.rename(columns={
        "id": "player_id",
        "full_name": "player_name"
    })[["player_id", "player_name"]]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE players CASCADE"))
        df.to_sql("players", conn, if_exists="append", index=False)

    print(f"Inserted {len(df)} players")


if __name__ == "__main__":
    engine = get_engine()
    ingest_teams(engine)
    ingest_players(engine)

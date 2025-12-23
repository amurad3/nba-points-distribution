from datetime import datetime, timedelta
from src.db import get_engine
from src.ingest_yesterday import ingest_games, ingest_boxscores

def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

if __name__ == "__main__":
    engine = get_engine()

    # Pick a range that gives players enough history
    start = datetime.strptime("2025-11-01", "%Y-%m-%d").date()
    end   = datetime.strptime("2025-12-21", "%Y-%m-%d").date()

    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        print(f"\n=== {ds} ===")
        game_ids = ingest_games(engine, ds)
        ingest_boxscores(engine, game_ids)

CREATE TABLE IF NOT EXISTS teams (
  team_id INTEGER PRIMARY KEY,
  team_abbr TEXT,
  team_name TEXT
);

CREATE TABLE IF NOT EXISTS players (
  player_id INTEGER PRIMARY KEY,
  player_name TEXT
);

CREATE TABLE IF NOT EXISTS games (
  game_id TEXT PRIMARY KEY,
  game_date DATE NOT NULL,
  season TEXT,
  home_team_id INTEGER,
  away_team_id INTEGER,
  status TEXT
);

CREATE TABLE IF NOT EXISTS player_game_stats (
  game_id TEXT REFERENCES games(game_id),
  player_id INTEGER REFERENCES players(player_id),
  team_id INTEGER,
  minutes REAL,
  points INTEGER,
  PRIMARY KEY (game_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_features_daily (
  as_of_date DATE NOT NULL,
  game_id TEXT,
  player_id INTEGER REFERENCES players(player_id),
  opponent_team_id INTEGER,
  home_flag INTEGER,
  rest_days INTEGER,

  rolling_pts_5 REAL,
  rolling_pts_10 REAL,
  pts_std_10 REAL,

  rolling_min_5 REAL,
  rolling_min_10 REAL,
  min_std_10 REAL,

  last_game_pts REAL,
  last_game_min REAL,

  PRIMARY KEY (as_of_date, game_id, player_id)
);

CREATE TABLE IF NOT EXISTS predictions_daily (
  as_of_date DATE NOT NULL,
  game_id TEXT,
  player_id INTEGER REFERENCES players(player_id),

  mu_pts REAL,
  sigma_pts REAL,

  p15 REAL,
  p20 REAL,
  p25 REAL,
  p30 REAL,

  model_version TEXT,
  created_ts TIMESTAMP DEFAULT NOW(),

  PRIMARY KEY (as_of_date, game_id, player_id)
);

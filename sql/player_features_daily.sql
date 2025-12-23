CREATE TABLE IF NOT EXISTS player_features_daily (
    feature_date DATE NOT NULL,
    player_id BIGINT NOT NULL,

    pts_mean_3 FLOAT,
    pts_std_3 FLOAT,
    min_mean_3 FLOAT,

    pts_mean_5 FLOAT,
    pts_std_5 FLOAT,
    min_mean_5 FLOAT,

    pts_mean_10 FLOAT,
    pts_std_10 FLOAT,
    min_mean_10 FLOAT,

    games_played_last_10 INT,
    days_since_last_game INT,

    PRIMARY KEY (feature_date, player_id)
);

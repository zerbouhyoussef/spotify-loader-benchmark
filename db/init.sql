CREATE TABLE IF NOT EXISTS artist (
    artist_id   SERIAL PRIMARY KEY,
    artist_uri  VARCHAR(100) UNIQUE NOT NULL,
    artist_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS album (
    album_id   SERIAL PRIMARY KEY,
    album_uri  VARCHAR(100) UNIQUE NOT NULL,
    album_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS playlist (
    playlist_id  SERIAL PRIMARY KEY,
    pid          INT UNIQUE NOT NULL,
    name         VARCHAR(255),
    num_tracks   INT,
    num_holdouts INT,
    num_samples  INT
);

CREATE TABLE IF NOT EXISTS track (
    track_id    SERIAL PRIMARY KEY,
    track_uri   VARCHAR(100) UNIQUE NOT NULL,
    track_name  VARCHAR(255),
    duration_ms INT,
    artist_id   INT REFERENCES artist(artist_id),
    album_id    INT REFERENCES album(album_id)
);

CREATE TABLE IF NOT EXISTS playlist_track (
    playlist_id INT REFERENCES playlist(playlist_id),
    track_id    INT REFERENCES track(track_id),
    pos         INT,
    PRIMARY KEY (playlist_id, track_id)
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    id          SERIAL PRIMARY KEY,
    loader_name VARCHAR(50) NOT NULL,
    duration_s  FLOAT,
    playlists   INT,
    tracks      INT,
    errors      INT,
    run_id      VARCHAR(50),
    run_at      TIMESTAMP DEFAULT NOW()
);
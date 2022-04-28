# spotify-projects
projects using spotify API


With Machine Learning to recommend songs to listen to!


# Step 1: Create Database in PostgreSQL

```sql
-- Creating the Track Table (Fact Table)
CREATE TABLE IF NOT EXISTS spotify_track(
    unique_id TEXT PRIMARY KEY NOT NULL,
    song_id TEXT NOT NULL,
    song_name TEXT,
    duration_ms INTEGER,
    url TEXT, -- link to
    date_time_played TIMESTAMP,
    album_id TEXT,
    artist_id TEXT,
    date_time_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- time we insert it into the table
    );

-- Creating the Album Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_album(
    album_id TEXT NOT NULL PRIMARY KEY,
    album_name TEXT,
    release_date TEXT,
    url TEXT,
    image_url TEXT
    );

-- Creating the Artist Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_artists(
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT,
    url TEXT);


-- Creating spotify audio analysis!!! (Dimension Table)
CREATE TABLE  spotify_audio_analysis(
    song_id TEXT NOT NULL PRIMARY KEY,
    acousticness decimal(5,4),
    danceability decimal(5,3),
    energy decimal(5,3),
    liveness decimal(5,3),
    loudness decimal(5,2),
    tempo decimal(5,2),
    valence decimal(5,3),
    speechiness decimal(5,5),
    instrumentalness decimal(5,5) );
```

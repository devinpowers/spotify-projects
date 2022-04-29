
-- Query for my top 5 Songs Last 7 Days By Time Listened.
SELECT   st.song_name, 
         Round(Sum(Cast(duration_ms AS DECIMAL)/60000),2) AS min_duration 
FROM     spotify_track                                    AS st 
WHERE    date_time_played > CURRENT_DATE - interval '7 days' 
GROUP BY st.song_name 
ORDER BY min_duration DESC limit 5;

--Function 

CREATE FUNCTION function_last_7_days_top_5_songs_duration() 
returns TABLE (song_name text, min_duration decimal) language plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT   st.song_name, 
           round(sum(cast(duration_ms AS decimal)/60000),2) AS min_duration 
  FROM     spotify_track                                    AS st 
  WHERE    date_time_played > CURRENT_DATE - interval '7 days' 
  GROUP BY st.song_name 
  ORDER BY min_duration DESC limit 5; 
  end;$$
  
  ----------------------------------------------------------------------------------------

--Most "Danceable Song Listened in the Last 7 Days

-- Top 5 Most Danceable Songs listened to the last 7 days 
SELECT st.song_name, sa.name AS artist_name, aas.danceability
FROM spotify_track AS st
INNER JOIN spotify_artists AS sa ON st.artist_id = sa.artist_id
JOIN spotify_audio_analysis AS aas ON st.song_id = aas.song_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY st.song_name, sa.name, danceability
ORDER BY danceability DESC
LIMIT 5;

--Function--:
CREATE FUNCTION function_most_danceable_songs()
RETURNS TABLE (song_name TEXT, artist_name TEXT, dance DECIMAL ) LANGUAGE plpgsql AS $$ 
BEGIN
	RETURN QUERY
	SELECT st.song_name, sa.name AS artist_name, aas.danceability AS dance
	FROM spotify_track AS st
	INNER JOIN spotify_artists AS sa ON st.artist_id = sa.artist_id
	JOIN spotify_audio_analysis AS aas ON st.song_id = aas.song_id
	WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
	GROUP BY st.song_name, sa.name, dance
	ORDER BY dance DESC
	LIMIT 5;
END;$$

---------------------------------------------------------------------------------

--  Query for Total Time Listened in Last 7 Days (Hours):
SELECT ROUND(SUM(CAST (duration_ms AS decimal)/3600000),2) AS total_time_listened_hours
FROM spotify_track
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days';

-- Function:
CREATE FUNCTION function_last_7_days_hrs_listened() 
RETURNS TABLE (total_time_listened_hrs decimal) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT   ROUND(SUM(CAST (st.duration_ms AS decimal)/3600000),2) AS total_time_listened_hrs 
  FROM     spotify_track AS st 
  WHERE    date_time_played > CURRENT_DATE - INTERVAL '7 days';
end;$$ 
---------------------------------------------------------------------------------

-- Most Popular Songs and Arists Names by Number of Plays
SELECT st.song_name, sa.name AS artist_name,COUNT(st.*) AS times_played
FROM spotify_track AS st
INNER JOIN spotify_artists AS sa 
ON st.artist_id = sa.artist_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY st.song_name, sa.name
ORDER BY times_played DESC
LIMIT 5;

-- Function: 
CREATE FUNCTION function_last_7_days_songs_artist_played() 
RETURNS TABLE (song_name TEXT, artist_name TEXT, times_played INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT st.song_name, sa.name AS artist_name,COUNT(st.*)::INT AS times_played
    FROM spotify_track AS st
    INNER JOIN spotify_artists AS sa 
    ON st.artist_id = sa.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY st.song_name, sa.name
    ORDER BY times_played DESC
    LIMIT 5;
end;$$ 
---------------------------------------------------------------------------------

-- Top Artists Listened To Query:

SELECT art.name, COUNT(track.*):: INT AS number_plays
    FROM spotify_schema.spotify_track AS track
    INNER JOIN spotify_schema.spotify_artists AS art ON track.artist_id=art.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY art.name
    ORDER BY number_plays DESC
    LIMIT 5;

-- Function:
CREATE FUNCTION function_last_7_days_artist_played() 
RETURNS TABLE (name TEXT, number_plays INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
 SELECT art.name, COUNT(track.*):: INT AS number_plays
    FROM  spotify_track AS track
    INNER JOIN spotify_artists AS art ON track.artist_id=art.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY art.name
    ORDER BY number_plays DESC
    LIMIT 5;
end;$$ 

---------------------------------------------------------------------------------

-- Create View for Top Decades of Music Played:

CREATE OR REPLACE VIEW track_decades AS
SELECT *,
CASE 
	WHEN subqry.release_year >= 1940 AND subqry.release_year <= 1949  THEN '1940''s'
	WHEN subqry.release_year >= 1950 AND subqry.release_year <= 1959  THEN '1950''s'
	WHEN subqry.release_year >= 1960 AND subqry.release_year <= 1969  THEN '1960''s'
	WHEN subqry.release_year >= 1970 AND subqry.release_year <= 1979  THEN '1970''s'
	WHEN subqry.release_year >= 1980 AND subqry.release_year <= 1989  THEN '1980''s'
	WHEN subqry.release_year >= 1990 AND subqry.release_year <= 1999  THEN '1990''s'
	WHEN subqry.release_year >= 2000 AND subqry.release_year <= 2009  THEN '2000''s'
	WHEN subqry.release_year >= 2010 AND subqry.release_year <= 2019  THEN '2010''s'
	WHEN subqry.release_year >= 2020 AND subqry.release_year <= 2029  THEN '2020''s'
ELSE 'Other'
END AS decade
FROM 
(SELECT album.album_id,album.name,album.release_date,track.unique_identifier,track.date_time_played,track.song_name,CAST(SPLIT_PART(release_date,'-',1) AS INT) AS release_year
FROM spotify_album AS album
INNER JOIN spotify_track AS track ON track.album_id = album.album_id) AS subqry;
---------------------------------------------------------------------------------

-- Query: Unique decades played during last week (7 days)
SELECT decade, COUNT(unique_identifier) AS total_plays
FROM track_decades
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY decade
ORDER BY total_plays DESC;

-- Function for above query
CREATE FUNCTION function_last_7_days_top_decades() 
RETURNS TABLE (decade TEXT, total_plays INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
SELECT track_decades.decade, COUNT(unique_identifier)::INT AS total_plays
FROM spotify_schema.track_decades
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY track_decades.decade
ORDER BY total_plays DESC;
end;$$ 

---------------------------------------------------------------------------------

-- Top ALbums playe this week

SELECT album.name,  COUNT(track) AS number_plays
FROM spotify_track AS track
INNER JOIN spotify_album AS album ON track.album_id = album.album_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY album.name
ORDER BY number_plays DESC
LIMIT 10;

--New Functions

CREATE FUNCTION function_albums()
 RETURNS TABLE (name TEXT, album_url TEXT, album_cover TEXT, number_plays INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
SELECT album.name, album.url AS album_url, album.image_url AS album_cover, COUNT(track.*)::INT AS number_plays
FROM spotify_track AS track
INNER JOIN spotify_album AS album ON track.album_id = album.album_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY album.name, album.url, album.image_url
ORDER BY number_plays DESC
LIMIT 3;
end;$$ 
---------------------------------------------------------------------------------

-- Top 5 LOUDEST  Songs listened to the last 7 days 
SELECT st.song_name, sa.name AS artist_name, aas.loudness
FROM spotify_track AS st
INNER JOIN spotify_artists AS sa ON st.artist_id = sa.artist_id
JOIN spotify_audio_analysis AS aas ON st.song_id = aas.song_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY st.song_name, sa.name, loudness
ORDER BY loudness ASC
LIMIT 5;

-------------------


-- Top 5 Good Feeling Songs listened to the last 7 days (High Valence)
SELECT st.song_name, sa.name AS artist_name, aas.valence
FROM spotify_track AS st
INNER JOIN spotify_artists AS sa ON st.artist_id = sa.artist_id
JOIN spotify_audio_analysis AS aas ON st.song_id = aas.song_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY st.song_name, sa.name, valence
ORDER BY valence DESC
LIMIT 10;



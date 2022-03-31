
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sys
import psycopg2
from sqlalchemy import create_engine
import sys


def spotify_etl_func():
        
    spotify_client_id = "766b1225eb5c49fda975d4a05f56a3f8" ## ADDD HERE
    spotify_client_secret = "986c5d4728bc45d4b174b393d9b43deb"     ### ADD HERE 
    spotify_redirect_url = "http://localhost"  

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                    client_secret=spotify_client_secret,
                                                    redirect_uri=spotify_redirect_url,
                                                    scope="user-read-recently-played"))

    recently_played = sp.current_user_recently_played(limit=50)

    #if the length of recently_played is 0 for some reason just exit the program

    if len(recently_played) ==0:
        sys.exit("No results recieved from Spotify")

    #Creating the Album Data Structure:
    album_dict = {}
    album_id = []
    album_name = []
    album_release_date = []
    album_total_tracks = []
    album_url = []

    for row in recently_played['items']:
        album_id.append(row['track']['album']['id'])

        album_name.append(row['track']['album']['name'])
        album_release_date.append(row['track']['album']['release_date'])
        album_total_tracks.append(row['track']['album']['total_tracks'])
        album_url.append(row['track']['album']['external_urls']['spotify'])
        
    album_dict = {'album_id':album_id,'name':album_name,'release_date':album_release_date, 'total_tracks':album_total_tracks,'url':album_url}






    #Creating the Artist Data Structure:
    #As we can see here this is another way to store data with using a dictionary of lists. Personally, for this project
    #I think using the strategy with the albums dicts(lists) is better. It allows for more functionality if we have to sort for example.
    # Additionally we do not need to make the temporary lists. There may be a more pythonic method to creating this but it is not my preferred method
    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played['items']:
        for key,value in item.items():
            if key == "track":
                for data_point in value['artists']:
                    id_list.append(data_point['id'])
                    name_list.append(data_point['name'])
                    url_list.append(data_point['external_urls']['spotify'])
    artist_dict = {'artist_id':id_list,'name':name_list,'url':url_list}




    song_dict = {}
    song_id = []
    song_name = []
    song_duration = []
    song_url = []
    song_popularity = []
    song_time_played = []
    album_id = []
    song_element = []
    artist_id = []

    for row in recently_played['items']:

        song_id.append(row['track']['id'])
        song_name.append(row['track']['name'])
        song_duration.append(row['track']['duration_ms'])
        song_url.append(row['track']['external_urls']['spotify'])
        song_popularity.append(row['track']['popularity'])
        song_time_played.append(row['played_at'])
        album_id.append(row['track']['album']['id'])
        artist_id.append(row['track']['album']['artists'][0]['id'])

    song_dict = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'date_time_played':song_time_played,'album_id':album_id,
                        'artist_id':artist_id
                        }
    

    #Now that we have these two lists and one dictionary ready lets convert them to DataFrames
    #We will need to do some cleaning and add our Unique ID for the Track
    #Then load into PostgresSQL from the dataframe

    #Album = We can also just remove duplicates here. We dont want to load two of the same albums just to have SQL drop it later
    album_df = pd.DataFrame.from_dict(album_dict)
    album_df = album_df.drop_duplicates(subset=['album_id'])

    album_df.to_csv(r"/Users/Devinpowers/Downloads/album_df_compare.csv")
    # print("Album Dataframe: ", album_df )


    #Artist = We can also just remove duplicates here. We dont want to load two of the same artists just to have SQL drop it later
    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    # print("Artist DF: ", artist_df)


    #Song Dataframe
    song_df = pd.DataFrame.from_dict(song_dict)
    #date_time_played is an object (data type) changing to a timestamp
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    #converting to my timezone of Central
    song_df['date_time_played'] = song_df['date_time_played'].dt.tz_convert('US/Central')
    #I have to remove the timezone part from the date/time/timezone.
    song_df['date_time_played'] = song_df['date_time_played'].astype(str).str[:-7]
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    #Creating a Unix Timestamp for Time Played. This will be one half of our unique identifier
    song_df['UNIX_Time_Stamp'] = (song_df['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
    # I need to create a new unique identifier column because we dont want to be insterting the same song played at the same song
    # I can have the same song multiple times in my database but I dont want to have the same song played at the same time
    song_df['unique_identifier'] = song_df['song_id'] + "-" + song_df['UNIX_Time_Stamp'].astype(str)
    song_df = song_df[['unique_identifier','song_id','song_name','duration_ms','url','popularity','date_time_played','album_id','artist_id']]
    # TESt saving to downloads

    # print("Song Dataframe: ", song_df)

    song_df.to_csv(r"/Users/Devinpowers/Downloads/test4.csv")


    #NOW We are going to Load PostgresSQL
   

    #NOW We are going to Load PostgresSQL

    #Loading the data into the Temporary Table
    conn = psycopg2.connect(host = "localhost", user = "devinpowers",port="5433", dbname = "spotify")
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://devinpowers@localhost:5433/spotify')
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()


    #TRACKS: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_track AS SELECT * FROM spotify_track LIMIT 0
    """)
    song_df.to_sql("tmp_track", con = engine, if_exists='append', index = False)
    #Moving data from temp table to production table

    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_track
        SELECT tmp_track.*
        FROM   tmp_track
        LEFT   JOIN spotify_track USING (unique_identifier)
        WHERE  spotify_track.unique_identifier IS NULL;
        
        DROP TABLE tmp_track""")

    conn.commit()

    #ALBUM: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_album LIMIT 0
    """)
    album_df.to_sql("tmp_album", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving from Temp Table to Production Table
    cur.execute(
    """
    INSERT INTO spotify_album
    SELECT tmp_album.*
    FROM   tmp_album
    LEFT   JOIN spotify_album USING (album_id)
    WHERE  spotify_album.album_id IS NULL;

    DROP TABLE tmp_album""")
    conn.commit()

    #Artist: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_artists LIMIT 0
    """)
    artist_df.to_sql("tmp_artist", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_artists
    SELECT tmp_artist.*
    FROM   tmp_artist
    LEFT   JOIN spotify_artists USING (artist_id)
    WHERE  spotify_artists.artist_id IS NULL;

    DROP TABLE tmp_artist""")
    conn.commit()

    return  'Finished ETL to csv file'

if __name__=='__main__':
    
    run = spotify_etl_func()
    print(run)

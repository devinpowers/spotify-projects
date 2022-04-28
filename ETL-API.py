#import spotipy, Spotify API Library
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
#import postsql things
import psycopg2
from sqlalchemy import create_engine
import sys


def spotify_func1(sp):
        
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                        client_secret=spotify_client_secret,
                                                        redirect_uri=spotify_redirect_url,
                                                        scope="user-read-recently-played"))

        # Will return 50 most recent played songs
        recently_played = sp.current_user_recently_played(limit=50)

        # Creating the Album Data Structure:
        album_dict = {}
        album_id = []
        album_name = []
        album_release_date = []
        album_total_tracks = []
        album_url = []
        album_image = []

        for row in recently_played['items']:
            album_id.append(row['track']['album']['id'])

            album_name.append(row['track']['album']['name'])
            album_release_date.append(row['track']['album']['release_date'])
            album_total_tracks.append(row['track']['album']['total_tracks'])
            album_url.append(row['track']['album']['external_urls']['spotify'])
            album_image.append(row['track']['album']['images'][1]['url'])

        album_dict = {'album_id':album_id,'name':album_name,'release_date':album_release_date, 'total_tracks':album_total_tracks,'url':album_url, 'image_url': album_image }


        # Creating the Artist Data Structure !

        artist_dict = {}
        id_list = []
        name_list = []
        url_list = []

        for row in recently_played['items']:

            id_list.append(row['track']['artists'][0]['id'])
            name_list.append(row['track']['artists'][0]['name'])
            url_list.append(row['track']['artists'][0]['external_urls']['spotify'])

        artist_dict = {'artist_id':id_list,'artist_name':name_list,'url':url_list} # Combine

        # For Song Track Data Structure 

        song_dict = {}
        song_id = []
        song_name = []
        song_duration = []
        song_url = []
        song_time_played = []
        album_id = []
        song_element = []
        artist_id = []

        for row in recently_played['items']:

            song_id.append(row['track']['id'])
            song_name.append(row['track']['name'])
            song_duration.append(row['track']['duration_ms'])
            song_url.append(row['track']['external_urls']['spotify'])

            song_time_played.append(row['played_at'])
            album_id.append(row['track']['album']['id'])
            artist_id.append(row['track']['album']['artists'][0]['id'])

        song_dict = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                            'date_time_played':song_time_played,'album_id':album_id,
                            'artist_id':artist_id
                            }
        
        Album_df = pd.DataFrame.from_dict(album_dict)
        Album_df = Album_df.drop_duplicates(subset=['album_id'])

        Artist_df = pd.DataFrame.from_dict(Artist_Dict)
        Artist_df = Artist_Df.drop_duplicates(subset=['artist_id'])

        Song_df = pd.DataFrame.from_dict(song_dict)
        
        Song_df['date_time_played'] = pd.to_datetime(Song_df['date_time_played'])
        Song_df['date_time_played'] = Song_df['date_time_played'].dt.tz_convert('US/Eastern')
        Song_df['date_time_played'] = Song_df['date_time_played'].astype(str).str[:-7]
        Song_df['date_time_played'] = pd.to_datetime(Song_df['date_time_played'])
        Song_df['UNIX_Time_Stamp'] = (Song_df['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
        Song_df['unique_id'] = Song_df['song_id'] + "-" + Song_df['UNIX_Time_Stamp'].astype(str)
        Song_df = Song_df[['unique_id','song_id','song_name','duration_ms','url','date_time_played','album_id','artist_id']]
        
        # Postgres
        conn = psycopg2.connect(host = "",user = "", port="", dbname = "")
        cur = conn.cursor()

        engine = create_engine('postgresql+psycopg2://@/') 
        conn_eng = engine.raw_connection()
        cur_eng = conn_eng.cursor()

        
        # Importing the Song_df into the SQL table

        cur_eng.execute(
        """
        CREATE TEMP TABLE temp_track AS SELECT * FROM spotify_track LIMIT 0
        """)
        Song_df.to_sql("temp_track", con = engine, if_exists='append', index = False)

        # Now we can move the data from the temp table into our actual table

        cur.execute(
        """
        INSERT INTO spotify_track
            SELECT *
            FROM   temp_track
            LEFT   JOIN spotify_track USING (unique_id)
            WHERE  spotify_track.unique_id IS NULL;

            DROP TABLE temp_track
        """)
        conn.commit()
        
        cur_eng.execute(
        """
        CREATE TEMP TABLE  temp_album AS SELECT * FROM spotify_album LIMIT 0
        """)
        album_df.to_sql("temp_album", con = engine, if_exists='append', index = False)
        conn_eng.commit()
        #Moving from Temp Table to Production Table
        cur.execute(
        """
        INSERT INTO spotify_album
        SELECT *
        FROM   temp_album
        LEFT   JOIN spotify_album USING (album_id)
        WHERE  spotify_album.album_id IS NULL;

        DROP TABLE temp_album""")
        conn.commit()


        # Artists Table
        cur_eng.execute(
        """
        CREATE TEMP TABLE temp_artist AS SELECT * FROM spotify_artists LIMIT 0
        """)
        artist_df.to_sql("temp_artist", con = engine, if_exists='append', index = False)
        conn_eng.commit()
        #Moving data from temp table to production table
        cur.execute(
        """
        INSERT INTO spotify_artists
        SELECT *
        FROM   temp_artist
        LEFT   JOIN spotify_artists USING (artist_id)
        WHERE  spotify_artists.artist_id IS NULL;

        DROP TABLE temp_artist""")
        conn.commit()
        
        return "ran function 1!"

def spotify_func2(sp):
        '''Audio'''
        recently_played = sp.current_user_recently_played(limit=50) 

        # Return dataframe of song_id
        song_id = []
        song_name = []

        for row in recently_played['items']:

            song_id.append(row['track']['id'])
            song_name.append(row['track']['name'])

        track_analysis = {}
        song_id_list = []
        acousticness = []
        danceability = []
        energy = []
        liveness = []
        loudness = []
        tempo = []
        valence = []
        instrumentalness = []
        speechiness = []

        for song in song_id:

            analysis_ = sp.audio_features(song)
            song_id_list.append(song)
            # Iterate through analysis and add columns to list!!!
            acousticness.append(analysis_[0]['acousticness'])
            danceability.append(analysis_[0]['danceability'])
            energy.append(analysis_[0]['energy'])
            liveness.append(analysis_[0]['liveness'])
            loudness.append(analysis_[0]['loudness'])
            tempo.append(analysis_[0]['tempo'])
            valence.append(analysis_[0]['valence'])
            speechiness.append(analysis_[0]['speechiness'])
            instrumentalness.append(analysis_[0]['instrumentalness'])


        track_analysis = {'song_id':song_id_list,'acousticness':acousticness,'danceability':danceability, 'energy': energy,'liveness': liveness, 'loudness' : loudness, 'tempo': tempo,'valence': valence, 'instrumentalness': instrumentalness, 'speechiness': speechiness }

        # Convert from Dictionary to Dataframe using Pandas, keep = False which is to drop the indexes

        track_analysis_df = pd.DataFrame.from_dict(track_analysis)
        track_analysis_df = track_analysis_df.drop_duplicates(subset=['song_id'], keep = False)
        
        
        '''Connect to our Postgres Database'''
        conn = psycopg2.connect(host = "localhost", user = "devinpowers",port="5433", dbname = "spotify")
        cur = conn.cursor()
        engine = create_engine('postgresql+psycopg2://devinpowers@localhost:5433/spotify') # this is 
        conn_eng = engine.raw_connection()
        cur_eng = conn_eng.cursor()


        cur_eng.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS tmp_spotify_audio_new AS SELECT * FROM spotify_audio_analysis LIMIT 0
        """)

        data_frame.to_sql("tmp_spotify_audio_new", con = engine, if_exists='append', index = False)

        #Moving data from temp table to production table
        cur.execute(
        """
            INSERT INTO spotify_audio_analysis
            SELECT * FROM tmp_spotify_audio_new
            WHERE tmp_spotify_audio_new.song_id NOT IN (SELECT song_id FROM spotify_audio_analysis );

            DROP TABLE tmp_spotify_audio_new
            """)
        conn.commit()

        return "Finished Function 2!"
        

        
       
if __name__ = "__main__":
        
        spotify_client_id = " " ## ADDD HERE
        spotify_client_secret = ""     ### ADD HERE 
        spotify_redirect_url = "http://localhost"  # might have to enter in this to redirect!
        
        # function 1
        func1 = spotify_func1(sp)
        print(func1)

        # function  2
        func1 = spotify_func2(sp)
        print(func2)
        
        # function 3 Email
        Import 
        
        

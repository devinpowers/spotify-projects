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
        
        
        return 'ran function 1!'

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



        
       
if __name__ = "__main__":
        
        spotify_client_id = " " ## ADDD HERE
        spotify_client_secret = ""     ### ADD HERE 
        spotify_redirect_url = "http://localhost"  # might have to enter in this to redirect!
        
        # function 1
        func1 = spotify_func1(sp)
        print(func1)

        # function  2
        
        # function 3 Email
        
        

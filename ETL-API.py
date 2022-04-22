import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sys
import psycopg2
from sqlalchemy import create_engine
import sys


def spotify_etl_func():
        
    spotify_client_id = "" ## ADDD HERE
    spotify_client_secret = ""     ### ADD HERE 
    spotify_redirect_url = "http://localhost"  

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                    client_secret=spotify_client_secret,
                                                    redirect_uri=spotify_redirect_url,
                                                    scope="user-read-recently-played"))

    recently_played = sp.current_user_recently_played(limit=50) # 50 requests is the limit I believe!  

    #print(recently_played)

    if len(recently_played) ==0:
        sys.exit("No results recieved from Spotify")

    #Creating the Album Data Structure:
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

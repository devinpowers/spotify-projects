'''
Script to extract audio analysis from songs in database
'''
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sys
import psycopg2
from sqlalchemy import create_engine
import sys
import json
import time


# Could pass list_Df that includes list of songs IDs in SQL tables

def spotify_etl_function_one():
        
    spotify_client_id = "766b1225eb5c49fda975d4a05f56a3f8" ## ADDD HERE
    spotify_client_secret = "986c5d4728bc45d4b174b393d9b43deb"     ### ADD HERE 
    spotify_redirect_url = "http://localhost"  

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                    client_secret=spotify_client_secret,
                                                    redirect_uri=spotify_redirect_url,
                                                    scope="user-read-recently-played"))

    recently_played = sp.current_user_recently_played(limit=20) # 50 requests is the limit I believe!  

    # Return datafram of song_id
    song_id = []
    song_name = []

    for row in recently_played['items']:

        song_id.append(row['track']['id'])
        song_name.append(row['track']['name'])

    # NOW what do I do with this SONG_ID???


    track_analysis = {}
    song_id_list = []
    acousticness = []
    danceability = []
    energy = []
    liveness = []
    loudness = []
    tempo = []
    valence = []




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

     



    track_analysis = {'song_id':song_id_list,'acousticness':acousticness,'danceability':danceability, 'energy': energy,'liveness': liveness, 'loudness' : loudness, 'tempo': tempo,'valence': valence }

    track_analysis_df = pd.DataFrame.from_dict(track_analysis)
    
    track_analysis_df.to_csv(r"/Users/Devinpowers/Desktop/Spotify-Projects/audio_analysis.csv")

    
if __name__ == '__main__':
    spotify_etl_function_one()
 

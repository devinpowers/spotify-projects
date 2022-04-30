

#Import things for postgres and formatting TEXT
import psycopg2
import smtplib,ssl
import json
from tabulate import tabulate
from datetime import datetime, timedelta

#Importing the module for Twitter!
import tweepy

# Keep Secret:
consumer_key=""
consumer_secret_key=""
access_token = ""
access_token_secret = ""

def tweet():

    # Connect to api
    # Connect to my database and then run Queries over
    conn = psycopg2.connect(host = "localhost", user = "devinpowers",port="5433", dbname = "spotify")

    cur = conn.cursor()
    #today = datetime.today().date()

    # Top 5 Songs I listened to this past Week

    top_5_songs_min = []

    # callproc: this method calls the stored procedure named by the proc_name argument.
    cur.callproc('function_last_7_days_top_5_songs_duration')
    for row in cur.fetchall():
        song_name = row[0]
        min_listened = float(row[1])
        element = [song_name,min_listened]
        
        top_5_songs_min.append(element)


    # testing printing list of top 5 songs

    # list to individual datatypes to store one song (so need 5)

    song1 =  top_5_songs_min[0][0]
    song2 =  top_5_songs_min[1][0]
    song3 =  top_5_songs_min[2][0]
    song4 =  top_5_songs_min[3][0]
    song5 =  top_5_songs_min[4][0]

    #
    album_url = []
    track_url= []
    album_name = [] 
    cur.callproc('function_albums')
    for row in cur.fetchall():
        
        album_name.append(row[0])
        album_url.append(row[1])
        track_url.append(row[2])

    # Call function to return total time listened to this past past!!
    cur.callproc('function_last_7_days_hrs_listened_')
    total_time_listened_hrs = float(cur.fetchone()[0])

    # Twitter Stuff!! Connect to the API

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Tweets to send:

    tweet1 = f"""\

    This week i've listen to {total_time_listened_hrs} of music! Here are my top 5 songs of the week!!

    1. {song1}
    2. {song2}
    3. {song3}
    4. {song4}
    5. {song5}
    """

    tweet2 =  f"""\
    Link to Top Album of the Week!
    {album_url[0]}

    """


    # List of tweets to send
    tweets = [tweet1, tweet2]


    try:
        for tweet in tweets:
            api.update_status(tweet)
            print("Posted!")

    except tweepy.error.TweepError as e:
        print(e)




if __name__ == "__main__":
    tweet()
    
    

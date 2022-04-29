import psycopg2
import smtplib,ssl
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
from datetime import datetime, timedelta

def weekly_email_function():
    
    # Connect to my database and then run Queries over
    conn = psycopg2.connect(host = "localhost", user = "devinpowers",port="5433", dbname = "spotify")

    cur = conn.cursor()
    today = datetime.today().date()
    six_days_ago = today - timedelta(days=6)

    #Top 5 Songs by Time Listened (MIN)
    top_5_songs_min = []

    # callproc: this method calls the stored procedure named by the proc_name argument.
    cur.callproc('function_last_7_days_top_5_songs_duration')
    for row in cur.fetchall():
        song_name = row[0]
        min_listened = float(row[1])
        element = [song_name,min_listened]
        top_5_songs_min.append(element)


    #Total Time Listened (HOURS)
    cur.callproc('function_last_7_days_hrs_listened_')
    total_time_listened_hrs = float(cur.fetchone()[0])


    #Top 5 Songs and Artists by Times Played
    #top_songs_art_played = [['Song Name ','Arist Name','Times Played']]
    top_songs_art_played = []

    cur.callproc('function_last_7_days_songs_artist_played')
    for row in cur.fetchall():
        song_name = row[0]
        artist_name = row[1]
        times_played = int(row[2])
        element = [song_name,artist_name,times_played]
        top_songs_art_played.append(element)

    #Top Artists Played
    top_art_played = []
    cur.callproc('function_last_7_days_artist_played')
    for row in cur.fetchall():
        artist_name = row[0]
        times_played = int(row[1])
        element = [artist_name,times_played]
        top_art_played.append(element)

    #Top Decades:
    top_decade_played = []
    cur.callproc('function_last_7_days_top_decades')
    for row in cur.fetchall():
        decade = row[0]
        times_played = int(row[1])
        element = [decade,times_played]
        top_decade_played.append(element)

    # Top 10 Albums played this week
    top_albums_played = []
    cur.callproc('function_last_7_days_album_played')
    for row in cur.fetchall():
        album_name = row[0]
        times_played = int(row[1])
        element = [album_name, times_played]
        top_albums_played.append(element)




    
    # Top 5 Most Danceable Songs played this Week!!!!
    top_dance_songs = []

    dance_album_cover = []
    dance_album_url = []

    cur.callproc('danceable_songs')
    for row in cur.fetchall():
        song_name = row[0]
        artist_name = row[1]
        dance_score = float(row[2])                # float() should cast string into a decimal

        element = [song_name, artist_name, dance_score]
        top_dance_songs.append(element)
        dance_album_url.append(row[3])
        dance_album_cover.append(row[4])

    
 
    album_url = []
    track_url= []
    album_name = []
    cur.callproc('function_albums')
    for row in cur.fetchall():
        
        album_name.append(row[0])
        album_url.append(row[1])
        track_url.append(row[2])

   
    
    #Sending the Email:
    port = 465
    password = "" # Add Password from GMAIL

    sender_email = "" # Email, Gmail
    receiver_email = "" # Who you're going to email!  

    message = MIMEMultipart("alternative")
    message["Subject"] = f"Spotify - Weekly Roundup - {today}"
    message["From"] = sender_email
    message["To"] = receiver_email


    text = f"""\
    Here are your stats for your weekly round up for Spotify. 
    Dates included: {six_days_ago} - {today}:

    """
    html = f"""\
    <html>
        <body>
            <h4>
            Hello (Insert you're name), here are your stats for your weekly round up for Spotify.
            </h4>
            <h4>
            Dates included: {six_days_ago} - {today}
            </h4>
            <h4>
            Total Time Listened: {total_time_listened_hrs} hours.
            </h4>
            <h4>
            Here are your Top Albums of the Week:
            </h4>
            <br>
            1. {album_name[0]}
            <br>
            2. {album_name[1]}
            <br>
            3. {album_name[2]}
            <br>
            4. {album_name[3]} 
            </br>
            
            <a href = {album_url[0]}>
             <img src= {track_url[0]}  alt="idk" width="300" height="300" class = "center">
            </a>
            
            <a href = {album_url[1]}>
            <img src= {track_url[1]}  alt="idk" width="300" height="300" class = "center">
            </a>
            
            <a href = {album_url[2]}>
            <img src= {track_url[2]}  alt="idk" width="300" height="300" class = "center">
            </a>

            <a href = {album_url[3]}>
            <img src= {track_url[3]}  alt="idk" width="300" height="300" class = "center">
            </a>

            <p>

            <h4>
            You listened to these songs and artists a lot here are your top 5!
            </h4>
            {tabulate(top_songs_art_played, headers = ["Song Name", "Artist Name", "Times Played"], tablefmt='html')}
            <h4>
            You spend a lot of time listening to these songs!
            </h4>
            {tabulate(top_5_songs_min, headers = ['Song Name', 'Time (Min)'], tablefmt='html')}
            <h4>
            Here are your Top 10 played artists!
            </h4>
            {tabulate(top_art_played, headers = ['Artist Name','Times Played'], tablefmt='html')}
            
            <h4>
            Here are your Top 10 most played albums!
            </h4>
            {tabulate(top_albums_played, headers = ['Album Name', 'Times Played'], tablefmt='html')}
            <h4>
            Here are your top decades:
            </h4>
            {tabulate(top_decade_played, headers = ['Decade','Times Played'], tablefmt='html')}
             <h4>
             Lastly your top danceable songs are here as the following:
            </h4>
            {tabulate(top_dance_songs, headers = ['Song Name', 'Artist', 'Danceability'], tablefmt='html')}
             <h4>
             Lets Dance to {top_dance_songs[0][0]} by {top_dance_songs[0][1]}
             </h4>
             <a href = {dance_album_url[0]}>
             <img src= {dance_album_cover[0]}  alt="idk" width="300" height="300" class = "center">
            </a>
            </p>
            <br>
              <h4>
             Machine Learning using recent played music to recommend new music....coming soon... 
             </h4>
             </br>
            
            
        </body>
    </html>"""

    part1 = MIMEText(text,"plain")
    part2 = MIMEText(html,"html")


    message.attach(part1)
    message.attach(part2)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com",port,context = context) as server:
        server.login("devinjpowers@gmail.com",password)
        server.sendmail(sender_email,receiver_email,message.as_string())

    
    print("Email Sent")

if __name__=='__main__':
    weekly_email_function()

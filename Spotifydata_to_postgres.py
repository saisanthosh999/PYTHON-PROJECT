import urllib

import psycopg2
import json
import base64
from io import StringIO
import csv
import pandas as pd
from dotenv import load_dotenv
from requests import post,get
import pymongo
import os

load_dotenv()

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

def get_token():
    auth_string = client_id + ':' + client_secret
    auth_bytes = auth_string.encode('utf-8')
    auth_base64 = str(base64.b64encode(auth_bytes),'utf-8')

    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization' : 'Basic ' + auth_base64,
        'Content-type' : 'application/x-www-form-urlencoded'
    }
    data = {'grant_type':'client_credentials'}
    result = post(url,headers=headers,data=data)
    json_result = json.loads(result.content)
    token = json_result['access_token']
    print("token with access_token is...",token)
    return token

def get_auth_header(token):
    return {"Authorization":"Bearer "+ token}

def serach_for_artist(token,artist_name):
    url = 'https://api.spotify.com/v1/search'
    headers = get_auth_header(token)
    query = f'?q={artist_name}&type=artist&limit=1'

    query_url = url + query
    result = get(query_url,headers=headers)
    json_result = json.loads(result.content)['artists']['items']
    if len(json_result) == 0:
        print('No artist with this name exists....')
        return None
    return json_result[0]

def get_songs_by_artist(token,artist_id):
    url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US'
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)['tracks']
    return json_result

token = get_token()
result = serach_for_artist(token,'ACDC')
artist_id = result['id']
songs = get_songs_by_artist(token,artist_id)
print(songs)
data = []
for json_element in songs:
    artists_info = json_element["artists"][0]
    id = artists_info["id"]
    name = artists_info["name"]
    type = artists_info["type"]

    is_playable = json_element["is_playable"]
    song_name = json_element["name"]
    release_date = json_element["album"]["release_date"]
    total_tracks = json_element["album"]["total_tracks"]
    disc_number = json_element["disc_number"]
    duration_ms = json_element["duration_ms"]
    popularity = json_element["popularity"]

    new_data = [
        (id, name, type, is_playable, song_name, release_date, total_tracks, disc_number, duration_ms, popularity)]
    pandas_df = pd.DataFrame(new_data,
                             columns=['ID', 'Artist_Name', 'Type', 'is_playable', 'Song Title', 'Release_Date',
                                      'Total_Tracks', 'Disc_Number', 'Duration(ms)', 'Popularity'])
    data.append(pandas_df)

combined_df = pd.concat(data)

# Write the combined DataFrame to CSV
csv_buffer = StringIO()
combined_df.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()

#postgresql config
# Replace 'your_username' and 'your_password' with your actual username and password
username = urllib.parse.quote_plus('postgres')
password = urllib.parse.quote_plus('Welcome@123#456')
postgres_uri = f"postgresql://{username}:{password}@localhost:5432/postgres"
conn = psycopg2.connect(postgres_uri)
cur = conn.cursor()

# Create a table (if it doesn't exist)
create_table_query = """
CREATE TABLE IF NOT EXISTS spotify_data (
    ID VARCHAR(255),
    Artist_Name VARCHAR(255),
    Type VARCHAR(255),
    is_playable BOOLEAN,
    Song_Title VARCHAR(255),
    Release_Date DATE,
    Total_Tracks INT,
    Disc_Number INT,
    Duration_ms INT,
    Popularity INT
)
"""
cur.execute(create_table_query)
conn.commit()

# Insert data into the table
for _, row in combined_df.iterrows():
    insert_query = """
    INSERT INTO spotify_data (ID, Artist_Name, Type, is_playable, Song_Title, Release_Date, Total_Tracks, Disc_Number, Duration_ms, Popularity)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = (
        row["ID"],
        row["Artist_Name"],
        row["Type"],
        row["is_playable"],
        row["Song Title"],
        row["Release_Date"],
        row["Total_Tracks"],
        row["Disc_Number"],
        row["Duration(ms)"],
        row["Popularity"]
    )
    cur.execute(insert_query, data)
print("data loaded to postgres...")
conn.commit()
conn.close()
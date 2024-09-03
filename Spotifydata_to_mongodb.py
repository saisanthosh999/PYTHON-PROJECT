import json
import base64
from io import StringIO
import csv
import pandas as pd
from dotenv import load_dotenv
from requests import post,get
import pymongo
import os
import timeit
start_time = timeit.default_timer()

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

#mongodb config
mongo_uri = "mongodb://localhost:27017"
client = pymongo.MongoClient(mongo_uri)
db = client["spotifydata"]
collection = db["data"]
for _, row in combined_df.iterrows():
    data = {
        "ID": row["ID"],
        "Artist_Name": row["Artist_Name"],
        "Type": row["Type"],
        "is_playable": row["is_playable"],
        "Song_Title": row["Song Title"],
        "Release_Date": row["Release_Date"],
        "Total_Tracks": row["Total_Tracks"],
        "Disc_Number": row["Disc_Number"],
        "Duration_ms": row["Duration(ms)"],
        "Popularity": row["Popularity"]
    }
    collection.insert_one(data)
print("data inserted into mongodb")
client.close()

end_time = timeit.default_timer()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")

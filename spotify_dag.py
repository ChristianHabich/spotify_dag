import urllib.request
from urllib.request import urlopen, Request
from urllib.parse import urlencode
import json
from datetime import datetime
from datetime import timedelta
import pandas as pd
import sqlalchemy
import sqlite3
from sqlalchemy.orm import sessionmaker
from os.path import join, dirname, abspath

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def spotify_pipeline():

    DATABASE_LOCATION = "sqlite:///{}".format(join(dirname(dirname(abspath(__file__))),'spotify_tracks.sqlite'))

    # 1 - refresh access token to authenticate to Spotify API
    refresh_token = "XXX"
    post_dict = {"grant_type": "refresh_token","refresh_token": refresh_token}
    url_encoded_data = urlencode(post_dict)
    post_data = url_encoded_data.encode("utf-8")

    base_64 = "XXX"
    headers={"Authorization": "Basic " + base_64}

    def post_request(url,data=None,headers=None):
        request = Request(url,data=post_data or {}, headers=headers or {})
        with urlopen(request, timeout=10) as response:
                return json.loads(response.read())

    refresh = post_request("https://accounts.spotify.com/api/token",data=post_data,headers=headers)
    token = refresh['access_token']

    # 2 - retrieve songs I listened to in the last 24 hours
    today = datetime.now()
    yesterday = today - timedelta(hours=24)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    headers = {
        "Authorization": f"Bearer {token}"
        }

    def make_request(url, headers=None):
        request = Request(url, headers=headers or {})
        with urlopen(request, timeout=10) as response:
                return json.loads(response.read())
        
    r = make_request("https://api.spotify.com/v1/me/player/recently-played?after={}&limit=50".format(yesterday_unix_timestamp),headers)

    # 3 - unwind json & store songs in dataframe
    song_names = []
    artist_names = []
    played_at = []
    timestamps = []
    
    for song in r["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
            
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)

    # 4 - check data integrity (transform stage)
    def check_if_valid_data(df: pd.DataFrame) -> bool:
        if df.empty:
            print("No songs downloaded. Finishing execution")
            return False
        
        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("Primary Key check is violated")
        
        if df.isnull().values.any():
            raise Exception("Null values found")
        
        return True

    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
    else:
         print("something went wrong during transform stage")

    # 5 - Load yesterday's songs into sqlite database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('spotify_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS spotify_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("spotify_tracks", engine, if_exists='append', index=False)
    except:
        print("Query ran already, yesterdays songs exist in database")

    conn.close()
    print("Closed database successfully")

# Run Spotify Pipeline every day via Airflow DAG

with DAG(
    dag_id='spotify_dag',
    schedule_interval='* 21 * * *',
    start_date=datetime(2024,5,5),
    catchup=False
) as dag:
    getmymusic = PythonOperator(
        task_id='spotify_pipeline',
        python_callable=spotify_pipeline
    )

    getmymusic
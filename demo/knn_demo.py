# DEFINE FUNCTIONS <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

def get_access_token(client_id:str, client_sc:str):
    import requests
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_sc}'.encode()
    response = requests.post('https://accounts.spotify.com/api/token', headers=headers, data=data).json()
    access_token = response['access_token']

    return access_token

def get_response(access_token:str, endpoint:str, params:dict=None):
    import requests, json

    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    if params != None:
        response = requests.get(url=url, params=params, headers=headers)
    else:
        response = requests.get(url=url, headers=headers)
    print(response)
    
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except json.decoder.JSONDecodeError:
            raise ValueError(f"API Server Error - {endpoint} - Invalid JSON content in response: {response.text}")
    else:
        raise ValueError(f"API Server Error - {endpoint} - Non-200 status code received: {response.status_code}")
    

def post_response(access_token:str, endpoint:str, data:dict=None):
    import requests

    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = requests.post(url=url, headers=headers)
    print(response)
    
    if response.status_code == 200:
        pass
    else:
        raise ValueError(f"API Server Error - {endpoint} - Non-200 status code received: {response.status_code}")
    
# INFOS <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

from configparser import ConfigParser

config = ConfigParser()
config.read("/home/hooniegit/git/Spotify-DemoProject/recommendation/demo/config.ini")

client_id = config.get("spotify", "client_id")
client_sc = config.get("spotify", "client_sc")
user_id = config.get("spotify", "user_id")

# START CODE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from math import ceil
import json

### Build Session
spark = SparkSession.builder \
    .master(config.get("spark", "master")) \
    .appName("pipeline_demo") \
    .getOrCreate()

### Create Access Token
access_token = get_access_token(client_id=client_id, client_sc=client_sc)

### Create Playlist Lists
endpoint = f"users/{user_id}/playlists"
params = {
    "limit": 50,
    "offset": 0
}

playlists = get_response(access_token=access_token, endpoint=endpoint, params=params)
json_string  = json.dumps(playlists)
json_rdd = spark.sparkContext.parallelize([json_string])
df_plinfo = spark.read.json(json_rdd, multiLine=True)

items = df_plinfo \
    .withColumn("items", explode("items")) \
    .select("items.id") \
    .rdd.flatMap(lambda x: x).collect()

### Create Playlist Item Lists
track_list = [] # <---------- "Need To Use"
for id in items:
    endpoint = f"playlists/{id}/tracks"
    playlist_spec = get_response(access_token=access_token, endpoint=endpoint)
    
    json_string  = json.dumps(playlist_spec)
    json_rdd = spark.sparkContext.parallelize([json_string])
    df_playlist_spec = spark.read.json(json_rdd, multiLine=True)
    
    ids = df_playlist_spec \
    .withColumn("items", explode("items")) \
    .select("items.track.id") \
    .rdd.flatMap(lambda x: x).collect()
    
    track_list += ids
    
    total = df_playlist_spec.select("total").first()[0]
    left = int(total)-100
    cnt = ceil(left/100)
    
    for i in range(cnt):
        offset = 100 + 100 * i
        params = {"offset":offset}
        
        playlist_spec = get_response(access_token=access_token, endpoint=endpoint, params=params)
        
        json_string  = json.dumps(playlist_spec)
        json_rdd = spark.sparkContext.parallelize([json_string])
        df_playlist_spec = spark.read.json(json_rdd, multiLine=True)
        
        ids = df_playlist_spec \
        .withColumn("items", explode("items")) \
        .select("items.track.id") \
        .rdd.flatMap(lambda x: x).collect()
        
        track_list += ids      

cnt = ceil(len(track_list)/50)

big_list = []
for j in range(cnt):
    big_list.append(track_list[j*50:(j+1)*50])

# Create Dataframe : main_df
main_df = None
cnt = 0
for small_list in big_list:
    
    print(cnt)
    
    tracks = ""
    for id in small_list:
        tracks += f",{id}"
    tracks = tracks[1:]
    
    endpoint = "tracks"
    params = {"ids":tracks}
    track = get_response(access_token=access_token, endpoint=endpoint, params=params)
    
    json_string  = json.dumps(track)
    json_rdd = spark.sparkContext.parallelize([json_string])
    df_tracks = spark.read.json(json_rdd, multiLine=True)
    
    df_tracks = spark.read.json(json_rdd, multiLine=True) \
        .withColumn("tracks", explode("tracks")) \
        .selectExpr("tracks.id",
                    "tracks.popularity")
    
    endpoint = "audio-features"
    params = {"ids":tracks}
    audio_features = get_response(access_token=access_token, endpoint=endpoint, params=params)
    
    json_string  = json.dumps(audio_features)
    json_rdd = spark.sparkContext.parallelize([json_string])
    df_audio_features = spark.read.json(json_rdd, multiLine=True) \
        .withColumn("audio_features", explode("audio_features")) \
        .selectExpr("audio_features.id",
                    "audio_features.key",
                    "audio_features.mode",
                    "audio_features.time_signature",
                    "audio_features.tempo",
                    "audio_features.acousticness",
                    "audio_features.danceability",
                    "audio_features.energy",
                    "audio_features.instrumentalness",
                    "audio_features.liveness",
                    "audio_features.loudness",
                    "audio_features.speechiness",
                    "audio_features.valence")
    
    result_track_df = df_tracks.join(df_audio_features, "id", "left")
    if cnt == 0:
        main_df = result_track_df
    else:
        main_df = main_df.union(result_track_df)
    cnt += 1

### Load Dataframe : df_dw
dw_tracks = spark.read.parquet("file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/tracks/main/*")
dw_audioFeatures = spark.read.parquet("file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/tracks/audio_features/*")
df_dw = dw_tracks.join(dw_audioFeatures, "id", "left")

### Union Dataframe : df
df = df_dw.union(main_df)

### Scale Dataframe : minmax_scaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler

selected_features = ["popularity", "key", "mode", "time_signature", "tempo", "acousticness", "danceability", "energy", "instrumentalness", "liveness", "loudness", "speechiness", "valence"]
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
df_assembled = assembler.transform(df)

minmax_scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
minmax_model = minmax_scaler.fit(df_assembled)
minmax_scaled_df = minmax_model.transform(df_assembled)

### Split Dataframe : train & test
from pyspark.sql.functions import col

minmax_scaled_train = minmax_scaled_df.filter(col("id").isin(track_list))
minmax_scaled_test = minmax_scaled_df.filter(~col("id").isin(track_list))

### Mege Datas into Group
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col

kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(minmax_scaled_train)

centers = model.clusterCenters()
print("Cluster Centers:")
for center in centers:
    print(center)

df_result = model.transform(minmax_scaled_test)
df_result.show()

df_prediction = df_result.select("features", "prediction").show()

df_mean_datas = df_result.groupBy("prediction").agg(
    col("prediction")
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os, sys
from configparser import ConfigParser

file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f"{file_dir}/../lib")
from spotify_libs import *
from spotify_request_libs import *

config = ConfigParser()
config.read(f"{file_dir}/../config/config.ini")

user_id = config.get("spotify", "user_id")
client_id = config.get("spotify", "client_id")
client_sc = config.get("spotify", "client_sc")
auth_code = config.get("spotify", "auth_code")
spark_master = config.get("spark", "master")

access_token = get_access_token(client_id=client_id, client_sc=client_sc)
auth_token = get_auth_token(auth_code=auth_code, client_id=client_id, client_sc=client_sc)

# Build Session
spark = SparkSession.builder \
    .master(spark_master) \
    .appName("recommendation_demo") \
    .getOrCreate()

# Create Train Dataframe
track_list = get_users_playlistIds(spark=spark, user_id=user_id, access_token=access_token)
df_train = create_dataframe_userPlaylists(spark=spark, access_token=access_token, track_list=track_list)

# Create Test Dataframe
dw_tracks = spark.read.parquet("file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/tracks/main/*")
dw_audioFeatures = spark.read.parquet("file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/tracks/audio_features/*")
df_test = dw_tracks.join(dw_audioFeatures, "id", "inner")

# Union Dataframe
df = df_test \
    .filter(~col("id").isin(track_list)) \
    .union(df_train)
    
# Standard Scale Dataframe
df = standard_scale_dataframe(df=df, columns=df.columns[1:])

# Split Dataframe
recommendationList = create_knn_recommendationList(df=df, track_list=track_list)

# Create Playlist
create_recommendPlaylists(user_id=user_id, auth_token=auth_token, recommendationList=recommendationList)

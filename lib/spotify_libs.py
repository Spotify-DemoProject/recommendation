import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spotify_request_libs import *

# get_users_playlistIds(spark=spark, user_id=.., access_token=..)
def get_users_playlistIds(spark, user_id:str, access_token:str):
    from pyspark.sql.functions import explode
    from math import ceil
    import json
    
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

    track_list = []
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

    return track_list

# create_dataframe_userPlaylists(spark=.., access_token=.., track_list=..)
def create_dataframe_userPlaylists(spark, access_token:str, track_list:list):
    from pyspark.sql.functions import explode
    from math import ceil
    import json
    
    big_list = []
    cnt = ceil(len(track_list)/50)
    for j in range(cnt):
        big_list.append(track_list[j*50:(j+1)*50])

    df = None
    cnt = 0
    for small_list in big_list:
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
            df = result_track_df
        else:
            df = df.union(result_track_df)
        cnt += 1
    
    return df

# standard_scale_dataframe(df=df, columns=df.columns[1:])
def standard_scale_dataframe(df, columns:list):
    from pyspark.sql.functions import mean, stddev, col

    for col_name in columns:
        mean_val = df.select(mean(col_name)).collect()[0][0]
        stds_val = df.select(stddev(col_name)).collect()[0][0]
        df = df.withColumn(col_name, (col(col_name) - mean_val) / stds_val)

    return df

# create_knn_recommendationList(df=df, track_list=..)
def create_knn_recommendationList(df, track_list:list):
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.clustering import KMeans
    from pyspark.sql.functions import col
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    import numpy as np

    scaled_train = df.filter(col("id").isin(track_list))
    scaled_test = df.filter(~col("id").isin(track_list))
        
    selected_features = ["popularity", "key", "mode", "time_signature", "tempo", "acousticness", "danceability", "energy", "instrumentalness", "liveness", "loudness", "speechiness", "valence"]

    # Define Assembler
    assembler = VectorAssembler(inputCols=selected_features, outputCol="features")

    # Assemble Features
    df_assembled_train = assembler.transform(scaled_train)
    df_assembled_test = assembler.transform(scaled_test)

    # Create Model == Train Dataset
    kmeans = KMeans(featuresCol="features", k=4, seed=4)
    model = kmeans.fit(df_assembled_train)

    # Check Centers
    centers = model.clusterCenters()
    print("Cluster Centers:")
    for center in centers:
        print(center)

    # Test Dataset
    df_result = model.transform(df_assembled_test)

    def calculate_distance(vector):
        return float(np.linalg.norm(vector.toArray() - numpy_coordinates))

    calculate_distance_udf = udf(calculate_distance, DoubleType())

    recommend_list = []
    for i in range(4):
        numpy_coordinates = centers[i]
        df_with_distance = df_result \
            .filter(f"prediction={i}") \
            .select("id", "features") \
            .withColumn('distance', calculate_distance_udf(col('features'))) \
            .orderBy("distance", ascending=True)
        
        collect_list = [row.id for row in df_with_distance.limit(5).collect()]
        recommend_list += collect_list
    
    return recommend_list

# create_recommendPlaylists(user_id=.., auth_token=.., recommendationList=..)
def create_recommendPlaylists(user_id:str, auth_token:str, recommendationList:list):
    from datetime import datetime

    today = datetime.now()
    first_day_of_month = today.replace(day=1)
    day_difference = (today - first_day_of_month).days
    week_number = (day_difference // 7) + 1
    year = today.strftime("%Y")
    month = today.strftime("%m")

    endpoint = f"users/{user_id}/playlists"
    playlist_name = f"{year}년 {month}월 {week_number}주차 월요일 추천 플레이리스트"
    playlist_description = "플레이리스트 기록을 기반으로 매주 월요일 음악을 추천해 드립니다 :)"
    data = {
        "name": playlist_name,
        "description": playlist_description,
        "public": False
    }

    returned = post_response(auth_token=auth_token, endpoint=endpoint, data=data)
    playlist_id = returned["id"]
    
    endpoint = f"playlists/{playlist_id}/tracks"
    data = {
        "uris": [f"spotify:track:{track_id}" for track_id in recommendationList],
        "position": 0
    }

    returned_final = post_response(auth_token=auth_token, endpoint=endpoint, data=data)
    print(returned_final)

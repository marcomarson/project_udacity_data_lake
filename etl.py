import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file #No inferschema when json, only csv
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/songs_table.parquet',partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists/df_artists_table.parquet")

    return df


def process_log_data(spark, input_data, output_data, song_data):
    # get filepath to log data file
    log_data =input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays #df[df['page']== 'NextSong']
    df = df.where(df.page == 'NextSong')

    # extract columns for users table  
    users_table = df.select(['userId','firstName','lastName','gender','level'])
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data+'users/users_table.parquet')

    # create timestamp column from original timestamp column
   #get_timestamp = udf()
    df =  df.withColumn("ts", to_timestamp(col("ts")/1000))
    df =  df.withColumnRenamed("ts", "start_time")
    
    df = df.withColumn("weekday", F.dayofweek("start_time"))
    df = df.withColumn("hour", F.hour("start_time"))
    df = df.withColumn("day", F.dayofmonth("start_time"))
    df = df.withColumn("week", F.weekofyear("start_time"))
    df = df.withColumn("month", F.month("start_time"))
    # extract columns to create time table
    time_table = df.select(['start_time','hour','day','week','month','weekday'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time/time_table.parquet')

    # read in song data to use for songplays table
    song_df = song_data
    #JOIN TWO dataframes
    df_total = df_log2.join(df_song, (df_log2["song"] == df_song["title"]) &
   ( df_log2["length"] == df_song["duration"]),"left")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_total.select(['start_time','userId','level','song_id','artist_id','sessionId','location','userAgent'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays/songplays_table.parquet')


def main():
    """
     Main Function that will execute and call the others. It is responsible for creating the psycopg2 connection
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a:#####/" ##Changed for privacy
    
    df_song = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, df_song)


if __name__ == "__main__":
    main()

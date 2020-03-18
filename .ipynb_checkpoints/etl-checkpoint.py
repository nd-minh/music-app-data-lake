import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql('''
          SELECT DISTINCT song_id, title, artist_id, year, duration
          FROM song_data 
          '''
          ).collect()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet",mode='overwrite',partitionBy=('year','artist_id'))

    # extract columns to create artists table
    artists_table = spark.sql('''
            SELECT DISTINCT 
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
            FROM song_data
    ''').collect()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet',mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data")
    df = spark.sql('''
            SELECT *
            FROM log_data
            WHERE page = 'NextSong'
    ''').collect()

    df.createOrReplaceTempView("log_data")
    # extract columns for users table    
    users_table = spark.sql('''
            SELECT DISTINCT 
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
            FROM log_data
    ''').collect()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet',mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('datetime', from_unixtime('start_time'))
    
    # extract columns to create time table
    time_table = spark.sql('''
            SELECT
            t.timestamp AS start_time,
            extract(hour from t.datetime),
            extract(day from t.datetime),
            extract(week from t.datetime),
            extract(month from t.datetime),
            extract(year from t.datetime),
            extract(weekday from t.datetime)
            FROM log_data AS t
    ''').collect()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time.parquet',mode='overwrite',partitionBy=('year','month'))

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
            SELECT 
            t1.datetime AS start_time,
            t1.userId as user_id,
            t1.level,
            t2.song_id,
            t2.artist_id,
            t1.sessionId as session_id,
            t1.location,
            t1.userAgent as user_agent       
            FROM log_data t1
            JOIN song_data t2 ON (t1.artist=t2.artist_name AND t1.length=t2.duration AND t1.song=t2.title)
    ''').collect()
    songplays_table = songplays_table.withColumn('songplay_id',monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays.parquet',mode='overwrite',partitionBy=('year','month'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://minh-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

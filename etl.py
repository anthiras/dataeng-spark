"""Spark ETL process"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create the SparkSession object, including the hadoop-aws package.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract raw song data, transform into song and artist tables, save into parquet files.

    Arguments:
        spark: The SparkSession object
        input_data: Path to input data where song_data is placed
        output_data: Output path where songs.parquest and artists.parquet will be saved
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(output_data + 'songs/songs.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude') \
        .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/artists.parquet', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Extract raw log data, transform into tables for users, songplays, and time, and save into parquet files.

    Arguments:
        spark: The SparkSession object
        input_data: Path to input data where log_data is placed
        output_data: Output path where songplays.parquest, users.parquet and time.parquet will be saved
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level') \
        .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/users.parquet', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000)
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(x)))
    df = df.withColumn('datetime', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('datetime')) \
        .withColumn('day', dayofmonth('datetime')) \
        .withColumn('week', weekofyear('datetime')) \
        .withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .withColumn('weekday', date_format('datetime', 'E')) \
        .select(['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']) \
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(output_data + 'time/time.parquet', mode='overwrite')

    # read in song data to use for songplays table
    spark.read.json(os.path.join(input_data, 'song_data')) \
        .createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table 
    df.withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .createOrReplaceTempView('log_data')
    songplays_table = spark.sql("""
        SELECT l.ts as start_time, l.userId as user_id, l.level, s.song_id, s.artist_id, l.sessionId as session_id, l.location, l.userAgent as user_agent, l.year, l.month
        FROM log_data l
        LEFT JOIN songs s ON s.artist_name = l.artist AND s.title = l.song
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(output_data + 'songplays/songplays.parquet', mode='overwrite')


def main():
    """
    Main ETL function, which will create a Spark function, and process song data and log data stored in an S3 bucket.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://atr-udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

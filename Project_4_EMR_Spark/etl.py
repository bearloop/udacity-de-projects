import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    This function builds and returns a spark process.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Reads song data from an S3 bucket and creates two tables: songs and artists. It writes these tables to the output destination.
    
    Arguments:
    spark: spark cursor object
    input_data: path to the S3 bucket containing the data
    output_data: path to the S3 bucket where the data will be saved
    '''
    # get filepath to song data file
    # demo: song_data = os.path.join(input_data+'song-data/A/A/A/*.json')
    song_data = os.path.join(input_data+'song-data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    print('Song data retrieved successfully')
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs_table.parquet'), mode='overwrite')# overwrite?
    print('OK - songs_table written to parquet')

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table.parquet'), mode='overwrite')
    print('OK - artists_table written to parquet')


def process_log_data(spark, input_data, output_data):
    '''
    Reads logs data from an S3 bucket and creates three tables: users, time and songplays. It writes these tables to the output destination.
    
    Arguments:
    spark: spark cursor object
    input_data: path to the S3 bucket containing the data
    output_data: path to the S3 bucket where the data will be saved
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data+'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    print('Log data retrieved successfully')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = df.select(col("userId").alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table.parquet'), mode='overwrite')
    print('OK - users_table written to parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : int(int(ts)/1000))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x))
    df = df.withColumn("datetime", get_timestamp(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select([df.timestamp.alias('start_time'),
                            hour(df.datetime).alias('hour'),
                            dayofmonth(df.datetime).alias('day'),
                            weekofyear(df.datetime).alias('week'),
                            month(df.datetime).alias('month'),
                            year(df.datetime).alias('year'),
                            dayofweek(df.datetime).alias('weekday')]).dropDuplicates()
            
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'time_table.parquet'), mode='overwrite')
    print('OK - time_table written to parquet')

    # read in song data to use for songplays table
    #demo: song_df = spark.read.json(os.path.join(input_data+'song-data/A/A/A/*.json'))
    song_df = spark.read.json(os.path.join(input_data+'song-data/*/*/*/*.json'))
    print('OK - songs data read successfully (again)')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,
                              (df.song == song_df.title) & \
                              (df.artist == song_df.artist_name) & \
                              (df.length == song_df.duration),
                              'left_outer').select(
                                                monotonically_increasing_id().alias('song_plays_id'),
                                                df.timestamp.alias('start_time'),
                                                col("userId").alias('user_id'),
                                                df.level,
                                                song_df.song_id,
                                                song_df.artist_id,
                                                col("sessionId").alias("session_id"),
                                                df.location,
                                                col("userAgent").alias("user_agent"),
                                                month(df.datetime).alias('month'),
                                                year(df.datetime).alias('year')).dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays_table.parquet'), mode='overwrite')
    print('OK - songplays_table written to parquet')


def main():
    
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    # output_data = '/home/workspace/output_demo/'
    output_data = 's3a://udacity-sparkify-bucket/' 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

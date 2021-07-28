import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, desc, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """

    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """

    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.format("json").load(
        song_data,
        inferSchema=True,
        header=True,
        multiLine=True
    )

    # extract columns to create songs table
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.\
        mode('overwrite').\
        partitionBy('year', 'artist_id').\
        parquet(output_data+'songs_table')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id',
                                  'artist_name as name',
                                  'artist_location as location',
                                  'artist_latitude as latitude',
                                  'artist_longitude as longitude')

    # write artists table to parquet files
    artists_table.write.\
        mode('overwrite').\
        parquet(output_data+'artists_table')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.format("json").load(
        log_data,
        inferSchema=True,
        header=True,
        multiLine=True
    )

    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table
    # it must not have any duplicates

    w = Window.partitionBy('userId').orderBy(desc('ts'))
    users_table = df.withColumn('order', row_number().over(w))
    users_table = users_table.where(users_table['order'] == 1) \
        .selectExpr('userId as user_id',
                    'firstName as first_name',
                    'lastName as last_name',
                    'gender',
                    'level')

    # write users table to parquet files
    users_table.write.mode('overwrite').\
        parquet(output_data+'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), TimestampType())
    df = df.withColumn('ts', get_timestamp(df['ts']))

    # extract columns to create time table
    time_table = df.select('ts').withColumn('hour', hour(col('ts'))) \
        .withColumn('year', year(col('ts')))\
        .withColumn('month', month(col('ts'))) \
        .withColumn('day', dayofmonth(col('ts'))) \
        .withColumn('week_of_year', weekofyear(col('ts'))) \
        .withColumn('weekday', date_format(col('ts'), 'E')) \
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.\
        mode('overwrite').\
        partitionBy('year', 'month').\
        parquet(output_data+'time_table')

    # read in song data to use for songplays table
    song_data = spark.read.format('json').load(
        input_data + "song_data/*/*/*/*.json",
        inferSchema=True,
        header=True,
        multiLine=True
    )

    # extract columns from joined song and log datasets to create songplays table
    spec = Window.partitionBy().orderBy('start_time')
    songplays_table = df.join(song_data,
                              (song_data.artist_name == df.artist)
                              & (song_data.title == df.song), 'inner') \
        .withColumn('start_time', get_timestamp(col('ts'))) \
        .selectExpr('start_time',
                    'userId as user_id',
                    'level',
                    'song_id',
                    'artist_id',
                    'sessionId as session_id',
                    'artist_location as location',
                    'userAgent as user_agent')\
        .withColumn('id', row_number().over(spec)).\
        withColumn('year', year(col('start_time'))).\
        withColumn('month', month(col('start_time')))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.\
        option('header', True).\
        partitionBy('year', 'month').\
        mode('overwrite').\
        parquet(output_data+'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-data-lake-abdu/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

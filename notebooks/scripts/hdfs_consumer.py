"""
Kafka -> HDFS Consumer : archive les donnees streaming dans HDFS
Equivalent au notebook 05 (sans les cellules d'arret)
Les donnees archivees servent au batch reprocessing (notebook 06)
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_JARS = ','.join([
    '/opt/spark-kafka-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/kafka-clients-3.4.1.jar',
    '/opt/spark-kafka-jars/commons-pool2-2.11.1.jar'
])

spark = SparkSession.builder \
    .appName('05_Kafka_to_HDFS') \
    .master('spark://spark-master:7077') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
    .config('spark.jars', KAFKA_JARS) \
    .config('spark.cores.max', '2') \
    .config('spark.executor.memory', '512m') \
    .getOrCreate()

trip_schema = StructType([
    StructField('date_id', DoubleType()),
    StructField('pickup_location_id', IntegerType()),
    StructField('dropoff_location_id', IntegerType()),
    StructField('weather_id', DoubleType()),
    StructField('trip_distance', DoubleType()),
    StructField('total_amount', DoubleType()),
    StructField('event_time', StringType())
])

kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'nyc-taxi-trips') \
    .option('startingOffsets', 'earliest') \
    .load()

trips_parsed = kafka_df \
    .select(from_json(col('value').cast('string'), trip_schema).alias('data')) \
    .select('data.*')

HDFS_OUTPUT = 'hdfs://namenode:9000/projet/streaming/december_trips'
CHECKPOINT = 'hdfs://namenode:9000/projet/streaming/_checkpoint_december'

query = trips_parsed.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', HDFS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT) \
    .trigger(processingTime='5 seconds') \
    .start()

print(f'Kafka -> HDFS demarre : {HDFS_OUTPUT}')

# Bloquer ici (tourne indefiniment)
spark.streams.awaitAnyTermination()

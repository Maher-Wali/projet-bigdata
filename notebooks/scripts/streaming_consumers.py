"""
Streaming Consumers : Kafka -> ClickHouse (temps reel)
Equivalent au notebook 04 (sans les cellules d'arret)
Lance 3 streams qui tournent en continu :
  1. Stats par borough -> ClickHouse
  2. Top zones -> ClickHouse
  3. Detection anomalies -> ClickHouse
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import builtins
import subprocess
subprocess.check_call(['pip', 'install', 'clickhouse-connect'])

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import clickhouse_connect
import pandas as pd

# ── Spark Session ──
KAFKA_JARS = ','.join([
    '/opt/spark-kafka-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/kafka-clients-3.4.1.jar',
    '/opt/spark-kafka-jars/commons-pool2-2.11.1.jar'
])

spark = SparkSession.builder \
    .appName('04_Kafka_Streaming_ClickHouse') \
    .master('spark://spark-master:7077') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
    .config('spark.jars', KAFKA_JARS) \
    .config('spark.cores.max', '3') \
    .config('spark.executor.memory', '1g') \
    .getOrCreate()

# Charger dimensions et stats batch
locations = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
ch_client = clickhouse_connect.get_client(host='clickhouse', port=8123)
batch_stats = ch_client.query_df("SELECT borough, nb_trips, avg_fare, avg_distance FROM batch_stats_borough")
ch_client.close()

print(f'Locations: {locations.count()} zones')
print(f'Stats batch: {len(batch_stats)} boroughs')

# ── Schema Kafka ──
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

trips_stream = kafka_df \
    .select(from_json(col('value').cast('string'), trip_schema).alias('data')) \
    .select('data.*')

print('Flux Kafka connecte')

# ══════════════════════════════════════════════════
# Stream 1 : Stats par borough
# ══════════════════════════════════════════════════
live_borough = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('borough')
    .agg(
        count('*').alias('nb_trips'),
        round(sum('total_amount'), 2).alias('total_revenue'),
        round(avg('trip_distance'), 2).alias('avg_distance'),
        round(avg('total_amount'), 2).alias('avg_fare')
    )
)

def write_borough_to_clickhouse(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    pdf = batch_df.toPandas()
    pdf = pdf.dropna(subset=['borough'])
    if pdf.empty:
        return
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    client.command("TRUNCATE TABLE stream_borough_stats")
    client.insert_df("stream_borough_stats", pdf)
    client.close()
    print(f"  [Batch {batch_id}] Borough stats: {len(pdf)} lignes -> ClickHouse")

query_borough = live_borough.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_borough_to_clickhouse) \
    .trigger(processingTime='2 seconds') \
    .start()

print('Stream 1 demarre : Stats borough -> ClickHouse')

# ══════════════════════════════════════════════════
# Stream 2 : Top zones
# ══════════════════════════════════════════════════
live_zones = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('zone', 'borough')
    .agg(count('*').alias('nb_trips'))
)

def write_zones_to_clickhouse(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    pdf = batch_df.toPandas()
    pdf = pdf.dropna(subset=['zone'])
    if pdf.empty:
        return
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    client.command("TRUNCATE TABLE stream_top_zones")
    client.insert_df("stream_top_zones", pdf)
    client.close()
    print(f"  [Batch {batch_id}] Top zones: {len(pdf)} lignes -> ClickHouse")

query_zones = live_zones.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_zones_to_clickhouse) \
    .trigger(processingTime='3 seconds') \
    .start()

print('Stream 2 demarre : Top zones -> ClickHouse')

# ══════════════════════════════════════════════════
# Stream 3 : Detection anomalies
# ══════════════════════════════════════════════════
live_anomaly = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('borough')
    .agg(
        count('*').alias('nb_trips'),
        round(avg('total_amount'), 2).alias('avg_fare'),
        round(avg('trip_distance'), 2).alias('avg_distance')
    )
)

def detect_anomalies(batch_df, batch_id):
    if batch_df.count() == 0 or batch_stats.empty:
        return
    pdf_stream = batch_df.toPandas()
    pdf_stream = pdf_stream.dropna(subset=['borough'])
    if pdf_stream.empty:
        return
    alerts = []
    import datetime
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for _, row in pdf_stream.iterrows():
        borough = row['borough']
        batch_row = batch_stats[batch_stats['borough'] == borough]
        if batch_row.empty:
            continue
        batch_fare = batch_row.iloc[0]['avg_fare']
        stream_fare = row['avg_fare']
        if batch_fare > 0:
            deviation = abs(stream_fare - batch_fare) / batch_fare * 100
            if deviation > 30:
                alerts.append({
                    'alert_time': now, 'alert_type': 'fare_deviation',
                    'borough': borough,
                    'message': f'Tarif moyen {stream_fare}$ vs historique {batch_fare}$ ({deviation:.0f}% deviation)',
                    'current_value': float(stream_fare),
                    'historical_value': float(batch_fare),
                    'deviation_pct': builtins.round(float(deviation), 1)
                })
        batch_dist = batch_row.iloc[0]['avg_distance']
        stream_dist = row['avg_distance']
        if batch_dist > 0:
            deviation_dist = abs(stream_dist - batch_dist) / batch_dist * 100
            if deviation_dist > 30:
                alerts.append({
                    'alert_time': now, 'alert_type': 'distance_deviation',
                    'borough': borough,
                    'message': f'Distance moy {stream_dist}mi vs historique {batch_dist}mi ({deviation_dist:.0f}% deviation)',
                    'current_value': float(stream_dist),
                    'historical_value': float(batch_dist),
                    'deviation_pct': builtins.round(float(deviation_dist), 1)
                })
    if alerts:
        client = clickhouse_connect.get_client(host='clickhouse', port=8123)
        client.insert_df("stream_alerts", pd.DataFrame(alerts))
        client.close()
        print(f"  [Batch {batch_id}] {len(alerts)} alertes detectees!")

query_anomaly = live_anomaly.writeStream \
    .outputMode('complete') \
    .foreachBatch(detect_anomalies) \
    .trigger(processingTime='3 seconds') \
    .start()

print('Stream 3 demarre : Detection anomalies')
print('\n=== 3 STREAMS ACTIFS - En attente de donnees Kafka ===')

# Bloquer ici (tourne indefiniment)
spark.streams.awaitAnyTermination()

"""
Streaming All-in-One : Kafka -> ClickHouse + HDFS
Un seul script, une seule SparkSession, 2 streams :
  1. Agregations (borough + zones + anomalies) -> ClickHouse (toutes les 5s)
  2. Archivage -> HDFS Parquet (toutes les 10s)
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import builtins
import time
import subprocess
subprocess.check_call(['pip', 'install', 'clickhouse-connect'])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, round as spark_round, from_json, broadcast, desc
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import clickhouse_connect
import pandas as pd

# ── Spark Session (unique, 4 cores) ──
KAFKA_JARS = ','.join([
    '/opt/spark-kafka-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/kafka-clients-3.4.1.jar',
    '/opt/spark-kafka-jars/commons-pool2-2.11.1.jar'
])

spark = SparkSession.builder \
    .appName('Streaming_Lambda') \
    .master('spark://spark-master:7077') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
    .config('spark.jars', KAFKA_JARS) \
    .config('spark.cores.max', '4') \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

# Charger dimensions et stats batch
locations = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
ch_client = clickhouse_connect.get_client(host='clickhouse', port=8123)
batch_stats = ch_client.query_df("SELECT borough, nb_trips, avg_fare, avg_distance FROM batch_stats_borough")
ch_client.close()

print(f'Locations: {locations.count()} zones')
print(f'Stats batch: {len(batch_stats)} boroughs')
sys.stdout.flush()

# ── Schema + Lecture Kafka ──
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
sys.stdout.flush()

# ══════════════════════════════════════
# Stream 1 : Stats borough (complete) -> ClickHouse
# Un seul stream avec foreachBatch qui ecrit
# borough stats + top zones + anomalies
# ══════════════════════════════════════
live_borough = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('borough', 'zone')
    .agg(
        count('*').alias('nb_trips'),
        spark_round(spark_sum('total_amount'), 2).alias('total_revenue'),
        spark_round(avg('trip_distance'), 2).alias('avg_distance'),
        spark_round(avg('total_amount'), 2).alias('avg_fare')
    )
)

def write_all_stats(batch_df, batch_id):
    try:
        pdf_all = batch_df.toPandas()
        if pdf_all.empty:
            return

        c = clickhouse_connect.get_client(host='clickhouse', port=8123)

        # 1. Borough stats (agreger par borough)
        pdf_borough = pdf_all.dropna(subset=['borough']).groupby('borough').agg({
            'nb_trips': 'sum',
            'total_revenue': 'sum',
            'avg_distance': 'mean',
            'avg_fare': 'mean'
        }).reset_index()
        pdf_borough['avg_distance'] = pdf_borough['avg_distance'].round(2)
        pdf_borough['avg_fare'] = pdf_borough['avg_fare'].round(2)
        pdf_borough['total_revenue'] = pdf_borough['total_revenue'].round(2)

        if not pdf_borough.empty:
            c.command("TRUNCATE TABLE stream_borough_stats")
            c.insert_df("stream_borough_stats", pdf_borough)
            total_trips = int(pdf_borough['nb_trips'].sum())
            print(f"  [B{batch_id}] Borough: {len(pdf_borough)} lignes, {total_trips} trajets")

        # 2. Top zones
        pdf_zones = pdf_all.dropna(subset=['zone'])[['zone', 'borough', 'nb_trips']]
        if not pdf_zones.empty:
            c.command("TRUNCATE TABLE stream_top_zones")
            c.insert_df("stream_top_zones", pdf_zones)

        # 3. Anomalies (comparer streaming vs batch historique)
        if not batch_stats.empty and not pdf_borough.empty:
            import datetime
            now = datetime.datetime.now()
            alerts = []
            for _, row in pdf_borough.iterrows():
                borough = row['borough']
                batch_row = batch_stats[batch_stats['borough'] == borough]
                if batch_row.empty:
                    continue
                bf = batch_row.iloc[0]['avg_fare']
                sf = row['avg_fare']
                if bf > 0:
                    dev = abs(sf - bf) / bf * 100
                    if dev > 30:
                        alerts.append({
                            'alert_time': now, 'alert_type': 'fare_deviation',
                            'borough': borough,
                            'message': f'Tarif {sf}$ vs historique {bf}$ ({dev:.0f}%)',
                            'current_value': float(sf), 'historical_value': float(bf),
                            'deviation_pct': builtins.round(float(dev), 1)
                        })
                bd = batch_row.iloc[0]['avg_distance']
                sd = row['avg_distance']
                if bd > 0:
                    dev_d = abs(sd - bd) / bd * 100
                    if dev_d > 30:
                        alerts.append({
                            'alert_time': now, 'alert_type': 'distance_deviation',
                            'borough': borough,
                            'message': f'Distance {sd}mi vs historique {bd}mi ({dev_d:.0f}%)',
                            'current_value': float(sd), 'historical_value': float(bd),
                            'deviation_pct': builtins.round(float(dev_d), 1)
                        })
            if alerts:
                c.insert_df("stream_alerts", pd.DataFrame(alerts))
                print(f"  [B{batch_id}] {len(alerts)} alertes!")

        c.close()
        sys.stdout.flush()
    except Exception as e:
        print(f"  [B{batch_id}] Erreur: {e}")
        sys.stdout.flush()

q1 = live_borough.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_all_stats) \
    .trigger(processingTime='5 seconds') \
    .start()
print('Stream 1 : Borough + Zones + Anomalies (5s)')
sys.stdout.flush()

# ══════════════════════════════════════
# Stream 2 : Archivage HDFS
# ══════════════════════════════════════
HDFS_OUTPUT = 'hdfs://namenode:9000/projet/streaming/december_trips'
CHECKPOINT = 'hdfs://namenode:9000/projet/streaming/_checkpoint_december'

q2 = trips_stream.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', HDFS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT) \
    .trigger(processingTime='10 seconds') \
    .start()
print('Stream 2 : Archivage HDFS (10s)')

print('\n=== 2 STREAMS ACTIFS ===')
print('Dashboard : http://localhost:8501\n')
sys.stdout.flush()

# ── Boucle infinie resiliente ──
while True:
    time.sleep(30)
    active = spark.streams.active
    print(f'  Streams actifs: {len(active)}')
    sys.stdout.flush()
    if len(active) == 0:
        print('  Tous les streams ont termine. Script reste en vie.')
        sys.stdout.flush()

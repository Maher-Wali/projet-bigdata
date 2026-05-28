"""
Streaming All-in-One : Kafka -> ClickHouse + HDFS
Un seul script, une seule SparkSession, 4 streams :
  1. Stats borough -> ClickHouse (toutes les 2s)
  2. Top zones -> ClickHouse (toutes les 3s)
  3. Anomalies -> ClickHouse (toutes les 3s)
  4. Archivage -> HDFS Parquet (toutes les 5s)
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import builtins
import time
import traceback
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
    .config('spark.sql.shuffle.partitions', '8') \
    .getOrCreate()

# Charger dimensions et stats batch
locations = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
ch_client = clickhouse_connect.get_client(host='clickhouse', port=8123)
batch_stats = ch_client.query_df("SELECT borough, nb_trips, avg_fare, avg_distance FROM batch_stats_borough")
ch_client.close()

print(f'Locations: {locations.count()} zones')
print(f'Stats batch: {len(batch_stats)} boroughs')

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

# ══════════════════════════════════════
# Stream 1 : Stats borough -> ClickHouse
# ══════════════════════════════════════
live_borough = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('borough')
    .agg(
        count('*').alias('nb_trips'),
        spark_round(spark_sum('total_amount'), 2).alias('total_revenue'),
        spark_round(avg('trip_distance'), 2).alias('avg_distance'),
        spark_round(avg('total_amount'), 2).alias('avg_fare')
    )
)

def write_borough(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            return
        pdf = batch_df.toPandas().dropna(subset=['borough'])
        if pdf.empty:
            return
        c = clickhouse_connect.get_client(host='clickhouse', port=8123)
        c.command("TRUNCATE TABLE stream_borough_stats")
        c.insert_df("stream_borough_stats", pdf)
        c.close()
        print(f"  [B{batch_id}] Borough: {len(pdf)} lignes, {pdf['nb_trips'].sum()} trajets")
    except Exception as e:
        print(f"  [B{batch_id}] Erreur borough: {e}")

q1 = live_borough.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_borough) \
    .trigger(processingTime='2 seconds') \
    .start()
print('Stream 1 : Borough stats (2s)')

# ══════════════════════════════════════
# Stream 2 : Top zones -> ClickHouse
# ══════════════════════════════════════
live_zones = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('zone', 'borough')
    .agg(count('*').alias('nb_trips'))
)

def write_zones(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            return
        pdf = batch_df.toPandas().dropna(subset=['zone'])
        if pdf.empty:
            return
        c = clickhouse_connect.get_client(host='clickhouse', port=8123)
        c.command("TRUNCATE TABLE stream_top_zones")
        c.insert_df("stream_top_zones", pdf)
        c.close()
        print(f"  [B{batch_id}] Zones: {len(pdf)} lignes")
    except Exception as e:
        print(f"  [B{batch_id}] Erreur zones: {e}")

q2 = live_zones.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_zones) \
    .trigger(processingTime='3 seconds') \
    .start()
print('Stream 2 : Top zones (3s)')

# ══════════════════════════════════════
# Stream 3 : Anomalies -> ClickHouse
# ══════════════════════════════════════
live_anomaly = (trips_stream
    .join(broadcast(locations),
          trips_stream.pickup_location_id == locations.location_id, 'left')
    .groupBy('borough')
    .agg(
        count('*').alias('nb_trips'),
        spark_round(avg('total_amount'), 2).alias('avg_fare'),
        spark_round(avg('trip_distance'), 2).alias('avg_distance')
    )
)

def detect_anomalies(batch_df, batch_id):
    try:
        if batch_df.count() == 0 or batch_stats.empty:
            return
        pdf_stream = batch_df.toPandas().dropna(subset=['borough'])
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
            c = clickhouse_connect.get_client(host='clickhouse', port=8123)
            c.insert_df("stream_alerts", pd.DataFrame(alerts))
            c.close()
            print(f"  [B{batch_id}] {len(alerts)} alertes!")
    except Exception as e:
        print(f"  [B{batch_id}] Erreur anomalies: {e}")

q3 = live_anomaly.writeStream \
    .outputMode('complete') \
    .foreachBatch(detect_anomalies) \
    .trigger(processingTime='3 seconds') \
    .start()
print('Stream 3 : Anomalies (3s)')

# ══════════════════════════════════════
# Stream 4 : Archivage HDFS
# ══════════════════════════════════════
HDFS_OUTPUT = 'hdfs://namenode:9000/projet/streaming/december_trips'
CHECKPOINT = 'hdfs://namenode:9000/projet/streaming/_checkpoint_december'

q4 = trips_stream.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', HDFS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT) \
    .trigger(processingTime='5 seconds') \
    .start()
print('Stream 4 : Archivage HDFS (5s)')

print('\n=== 4 STREAMS ACTIFS ===')
print('Dashboard : http://localhost:8501\n')

# ── Boucle infinie resiliente ──
while True:
    time.sleep(30)
    active = spark.streams.active
    print(f'  Streams actifs: {len(active)}')
    if len(active) == 0:
        print('  Tous les streams ont termine. Script reste en vie.')

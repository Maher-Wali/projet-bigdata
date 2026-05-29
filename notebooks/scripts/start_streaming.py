"""
Start Streaming : Producer + Consumers tout-en-un
Combine les notebooks 03, 04 et 05 en un seul script :
  - Producer Kafka (Decembre, rafales de 200 msg / 50ms)
  - Stream 1 : Stats borough    -> ClickHouse (toutes les 1s)
  - Stream 2 : Top zones        -> ClickHouse (toutes les 1s)
  - Stream 3 : Anomalies        -> ClickHouse (toutes les 1s)
  - Stream 4 : Archivage HDFS   -> Parquet    (toutes les 5s)
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import builtins
import time
import threading
import traceback
import subprocess
import datetime

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)

print('\n' + '='*60)
print('   NYC Taxi - Start Streaming')
print('='*60)
print()

print('[INIT] Installation des dependances...', flush=True)
subprocess.check_call(['pip', 'install', 'kafka-python', 'clickhouse-connect'], stdout=subprocess.DEVNULL)
print('[INIT] Dependances OK')

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, round as spark_round, from_json, broadcast
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import clickhouse_connect
import pandas as pd
import json, csv

# ── Config ──
KAFKA_BROKER   = 'kafka:29092'
TOPIC          = 'nyc-taxi-trips'
CSV_PATH       = '/home/jovyan/data/nyc-taxi/fact_trips_final.csv'
HDFS_OUTPUT    = 'hdfs://namenode:9000/projet/streaming/december_trips'
CHECKPOINT     = 'hdfs://namenode:9000/projet/streaming/_checkpoint_december'

KAFKA_JARS = ','.join([
    '/opt/spark-kafka-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
    '/opt/spark-kafka-jars/kafka-clients-3.4.1.jar',
    '/opt/spark-kafka-jars/commons-pool2-2.11.1.jar'
])

log('[SPARK] Connexion au cluster Spark...')
spark = SparkSession.builder \
    .appName('Start_Streaming') \
    .master('spark://spark-master:7077') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
    .config('spark.jars', KAFKA_JARS) \
    .config('spark.cores.max', '4') \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.shuffle.partitions', '8') \
    .getOrCreate()
log('[SPARK] Session Spark OK')

# ── Dimensions et stats batch ──
log('[SPARK] Chargement des dimensions (locations, batch_stats)...')
locations   = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
ch_client   = clickhouse_connect.get_client(host='clickhouse', port=8123)
batch_stats = ch_client.query_df("SELECT borough, nb_trips, avg_fare, avg_distance FROM batch_stats_borough")
ch_client.close()
log(f'[SPARK] Locations : {locations.count()} zones | Stats batch : {len(batch_stats)} boroughs')

# ── Schema Kafka ──
trip_schema = StructType([
    StructField('date_id',             DoubleType()),
    StructField('pickup_location_id',  IntegerType()),
    StructField('dropoff_location_id', IntegerType()),
    StructField('weather_id',          DoubleType()),
    StructField('trip_distance',       DoubleType()),
    StructField('total_amount',        DoubleType()),
    StructField('event_time',          StringType())
])

# ── Lecture Kafka ──
kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('subscribe', TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

trips_stream = kafka_df \
    .select(from_json(col('value').cast('string'), trip_schema).alias('data')) \
    .select('data.*')

log('[KAFKA] Flux Kafka connecte -> topic: ' + TOPIC)

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

log('[CONSUMER] Demarrage Stream 1 : Borough stats -> ClickHouse...')
q1 = live_borough.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_borough) \
    .trigger(processingTime='1 seconds') \
    .start()
log('[CONSUMER] Stream 1 actif (trigger 1s)')

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

log('[CONSUMER] Demarrage Stream 2 : Top zones -> ClickHouse...')
q2 = live_zones.writeStream \
    .outputMode('complete') \
    .foreachBatch(write_zones) \
    .trigger(processingTime='1 seconds') \
    .start()
log('[CONSUMER] Stream 2 actif (trigger 1s)')

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
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for _, row in pdf_stream.iterrows():
            borough  = row['borough']
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

log('[CONSUMER] Demarrage Stream 3 : Anomalies -> ClickHouse...')
q3 = live_anomaly.writeStream \
    .outputMode('complete') \
    .foreachBatch(detect_anomalies) \
    .trigger(processingTime='1 seconds') \
    .start()
log('[CONSUMER] Stream 3 actif (trigger 1s)')

# ══════════════════════════════════════
# Stream 4 : Archivage HDFS
# ══════════════════════════════════════
log('[CONSUMER] Demarrage Stream 4 : Archivage -> HDFS Parquet...')
q4 = trips_stream.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', HDFS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT) \
    .trigger(processingTime='5 seconds') \
    .start()
log('[CONSUMER] Stream 4 actif (trigger 5s)')

print()
print('  Stream 1 : Borough stats  -> ClickHouse (1s)')
print('  Stream 2 : Top zones      -> ClickHouse (1s)')
print('  Stream 3 : Anomalies      -> ClickHouse (1s)')
print('  Stream 4 : Archivage HDFS -> Parquet    (5s)')
print()
log('[WAIT] 4 consumers actifs. Lancement du producer dans 5s...')
time.sleep(5)

# ══════════════════════════════════════
# Producer Kafka (thread separé)
# ══════════════════════════════════════
def run_producer():
    BATCH_SIZE = 200
    LOG_EVERY  = 1000
    DELAY      = 0.05

    log(f'[PRODUCER] Connexion a Kafka ({KAFKA_BROKER})...')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=32768,
        linger_ms=5,
        buffer_memory=67108864
    )
    log(f'[PRODUCER] Connecte. Mode rafale : {BATCH_SIZE} msg/rafale, pause {DELAY}s (~4000 msg/s)')

    count = 0
    skipped = 0
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_id = float(row['data_id']) if row['data_id'] else None
            if date_id is None or date_id < 336:
                skipped += 1
                continue
            message = {
                'date_id':             date_id,
                'pickup_location_id':  int(row['localizacao_partida_id']),
                'dropoff_location_id': int(row['localizacao_chegada_id']),
                'weather_id':          float(row['clima_id']) if row['clima_id'] else None,
                'trip_distance':       float(row['distancia_viagem']),
                'total_amount':        float(row['valor_total']),
                'event_time':          datetime.datetime.now().isoformat()
            }
            producer.send(TOPIC, value=message)
            count += 1
            if count % LOG_EVERY == 0:
                log(f'[PRODUCER] {count} messages envoyes...')
            if count % BATCH_SIZE == 0:
                producer.flush()
                time.sleep(DELAY)

    producer.flush()
    producer.close()
    log(f'[PRODUCER] Termine : {count} messages envoyes ({skipped} ignores)')

producer_thread = threading.Thread(target=run_producer, daemon=True)
producer_thread.start()
log('[PRODUCER] Thread demarre')
print()
print('  Pour arreter : docker exec jupyter pkill -f start_streaming.py')
print()

# ── Boucle de supervision ──
while True:
    time.sleep(30)
    active = spark.streams.active
    log(f'[STATUS] Consumers actifs: {len(active)}/4 | Producer: {"en cours" if producer_thread.is_alive() else "termine"}')
    if len(active) == 0:
        log('[STATUS] Tous les streams ont termine.')

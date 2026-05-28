"""
Batch Reprocessing : retraite periodiquement les donnees streaming accumulees dans HDFS
Equivalent au notebook 06
Produit des resultats exacts vs les approximations du streaming
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import subprocess
subprocess.check_call(['pip', 'install', 'clickhouse-connect'])

import builtins
import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import clickhouse_connect
import pandas as pd

spark = SparkSession.builder \
    .appName('06_Batch_Reprocessing') \
    .master('spark://spark-master:7077') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
    .config('spark.sql.shuffle.partitions', '6') \
    .getOrCreate()

ch_client = clickhouse_connect.get_client(host='clickhouse', port=8123)
locations = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
dates = spark.read.parquet('hdfs://namenode:9000/projet/silver/dates_clean/')

print('Batch Reprocessing pret')

HDFS_STREAMING_PATH = 'hdfs://namenode:9000/projet/streaming/december_trips'
BATCH_INTERVAL_SECONDS = 30
NB_CYCLES = 5


def run_batch_reprocessing(cycle_num):
    start = datetime.datetime.now()
    print(f'\n{"="*60}')
    print(f'CYCLE {cycle_num} - {start.strftime("%H:%M:%S")}')
    print(f'{"="*60}')

    try:
        trips = spark.read.parquet(HDFS_STREAMING_PATH)
        nb_rows = trips.count()
        print(f'Donnees accumulees : {nb_rows} lignes')
    except Exception as e:
        print(f'Pas encore de donnees dans HDFS : {e}')
        return

    if nb_rows == 0:
        print('Aucune donnee a traiter')
        return

    # Stats par borough
    stats_borough = (trips
        .join(broadcast(locations),
              trips.pickup_location_id == locations.location_id, 'left')
        .groupBy('borough')
        .agg(
            count('*').alias('nb_trips'),
            round(sum('total_amount'), 2).alias('total_revenue'),
            round(avg('trip_distance'), 2).alias('avg_distance'),
            round(avg('total_amount'), 2).alias('avg_fare')
        )
        .orderBy(desc('nb_trips'))
    )
    stats_borough.show()
    pdf_borough = stats_borough.toPandas().dropna(subset=['borough'])
    ch_client.command('TRUNCATE TABLE reprocess_borough_stats')
    ch_client.insert_df('reprocess_borough_stats', pdf_borough)

    # Top zones
    top_zones = (trips
        .join(broadcast(locations),
              trips.pickup_location_id == locations.location_id, 'left')
        .groupBy('zone', 'borough')
        .agg(count('*').alias('nb_trips'))
        .orderBy(desc('nb_trips'))
        .limit(10)
    )
    top_zones.show(truncate=False)
    pdf_zones = top_zones.toPandas().dropna(subset=['zone'])
    ch_client.command('TRUNCATE TABLE reprocess_top_zones')
    ch_client.insert_df('reprocess_top_zones', pdf_zones)

    # Stats par jour
    stats_daily = (trips
        .join(broadcast(dates), trips.date_id == dates.date_id, 'left')
        .groupBy('day')
        .agg(
            count('*').alias('nb_trips'),
            round(sum('total_amount'), 2).alias('total_revenue'),
            round(avg('total_amount'), 2).alias('avg_fare')
        )
        .orderBy('day')
    )
    stats_daily.show()
    pdf_daily = stats_daily.toPandas().dropna(subset=['day'])
    ch_client.command('TRUNCATE TABLE reprocess_daily_stats')
    ch_client.insert_df('reprocess_daily_stats', pdf_daily)

    # Log du cycle
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    log_entry = {
        'cycle_num': cycle_num,
        'executed_at': end.strftime('%Y-%m-%d %H:%M:%S'),
        'rows_processed': nb_rows,
        'duration_seconds': builtins.round(duration, 1),
        'boroughs_found': len(pdf_borough),
        'zones_found': len(pdf_zones)
    }
    ch_client.insert_df('reprocess_log', pd.DataFrame([log_entry]))
    print(f'Cycle {cycle_num} termine en {builtins.round(duration, 1)}s - {nb_rows} lignes')


# Boucle de reprocessing
print(f'Intervalle : {BATCH_INTERVAL_SECONDS}s | Cycles : {NB_CYCLES}')
cycle = 1
while cycle <= NB_CYCLES:
    run_batch_reprocessing(cycle)
    if cycle >= NB_CYCLES:
        break
    print(f'Prochain batch dans {BATCH_INTERVAL_SECONDS}s...')
    time.sleep(BATCH_INTERVAL_SECONDS)
    cycle += 1

print(f'\nBATCH REPROCESSING TERMINE - {cycle} cycles')
ch_client.close()
spark.stop()

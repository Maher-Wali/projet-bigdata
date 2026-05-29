"""
Batch Watcher : surveille les declenchements de batch reprocessing
- Bouton du dashboard → detecte une ligne dans batch_trigger
- Automatique → toutes les AUTO_INTERVAL_HOURS heures
N'utilise PAS de SparkSession permanente (libere les cores pour le streaming)
Cree une session uniquement quand un batch est demande, puis la ferme.
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
import clickhouse_connect
import pandas as pd

# ══════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════
AUTO_INTERVAL_HOURS = 4
POLL_INTERVAL_SECONDS = 10
HDFS_STREAMING_PATH = 'hdfs://namenode:9000/projet/streaming/december_trips'


def get_ch_client():
    return clickhouse_connect.get_client(host='clickhouse', port=8123)


def get_next_cycle_num():
    client = get_ch_client()
    result = client.query('SELECT max(cycle_num) FROM reprocess_log')
    client.close()
    val = result.result_rows[0][0]
    return (val or 0) + 1


def run_batch_reprocessing(trigger_source):
    """Cree une SparkSession temporaire, execute le batch, puis la ferme."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import count, sum as spark_sum, avg, round as spark_round, desc, broadcast

    cycle_num = get_next_cycle_num()
    start = datetime.datetime.now()
    print(f'\n{"="*60}')
    print(f'BATCH REPROCESSING - Cycle {cycle_num}')
    print(f'Declenchement : {trigger_source}')
    print(f'Heure : {start.strftime("%H:%M:%S")}')
    print(f'{"="*60}')

    # Creer une SparkSession temporaire (2 cores seulement)
    spark = SparkSession.builder \
        .appName(f'BatchReprocess_Cycle{cycle_num}') \
        .master('spark://spark-master:7077') \
        .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000') \
        .config('spark.sql.shuffle.partitions', '4') \
        .config('spark.cores.max', '2') \
        .config('spark.executor.memory', '512m') \
        .getOrCreate()

    try:
        locations = spark.read.parquet('hdfs://namenode:9000/projet/silver/locations_clean/')
        dates = spark.read.parquet('hdfs://namenode:9000/projet/silver/dates_clean/')

        trips = spark.read.parquet(HDFS_STREAMING_PATH)
        nb_rows = trips.count()
        print(f'Donnees dans HDFS : {nb_rows} lignes')

        if nb_rows == 0:
            print('Aucune donnee a traiter')
            spark.stop()
            return

        client = get_ch_client()

        # Stats par borough
        stats_borough = (trips
            .join(broadcast(locations),
                  trips.pickup_location_id == locations.location_id, 'left')
            .groupBy('borough')
            .agg(
                count('*').alias('nb_trips'),
                spark_round(spark_sum('total_amount'), 2).alias('total_revenue'),
                spark_round(avg('trip_distance'), 2).alias('avg_distance'),
                spark_round(avg('total_amount'), 2).alias('avg_fare')
            )
            .orderBy(desc('nb_trips'))
        )
        stats_borough.show()
        pdf_borough = stats_borough.toPandas().dropna(subset=['borough'])
        client.command('TRUNCATE TABLE reprocess_borough_stats')
        client.insert_df('reprocess_borough_stats', pdf_borough)

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
        client.command('TRUNCATE TABLE reprocess_top_zones')
        client.insert_df('reprocess_top_zones', pdf_zones)

        # Stats par jour
        stats_daily = (trips
            .join(broadcast(dates), trips.date_id == dates.date_id, 'left')
            .groupBy('day')
            .agg(
                count('*').alias('nb_trips'),
                spark_round(spark_sum('total_amount'), 2).alias('total_revenue'),
                spark_round(avg('total_amount'), 2).alias('avg_fare')
            )
            .orderBy('day')
        )
        stats_daily.show()
        pdf_daily = stats_daily.toPandas().dropna(subset=['day'])
        client.command('TRUNCATE TABLE reprocess_daily_stats')
        client.insert_df('reprocess_daily_stats', pdf_daily)

        # Log du cycle
        end = datetime.datetime.now()
        duration = (end - start).total_seconds()
        log_entry = {
            'cycle_num': cycle_num,
            'executed_at': end,
            'rows_processed': nb_rows,
            'duration_seconds': builtins.round(duration, 1),
            'boroughs_found': len(pdf_borough),
            'zones_found': len(pdf_zones)
        }
        client.insert_df('reprocess_log', pd.DataFrame([log_entry]))
        client.close()

        print(f'\nCycle {cycle_num} termine en {builtins.round(duration, 1)}s')
        print(f'  {nb_rows} lignes traitees')

    except Exception as e:
        import traceback
        print(f'Erreur batch: {e}')
        traceback.print_exc()
        # Write a failure entry so the dashboard shows something went wrong
        try:
            end = datetime.datetime.now()
            duration = (end - start).total_seconds()
            err_client = get_ch_client()
            err_client.insert_df('reprocess_log', pd.DataFrame([{
                'cycle_num': cycle_num,
                'executed_at': end,
                'rows_processed': -1,
                'duration_seconds': builtins.round(duration, 1),
                'boroughs_found': 0,
                'zones_found': 0
            }]))
            err_client.close()
        except Exception as log_err:
            print(f'Impossible d\'ecrire le log d\'erreur: {log_err}')
    finally:
        spark.stop()
        print('SparkSession fermee (cores liberes)')


def check_manual_trigger():
    try:
        client = get_ch_client()
        result = client.query('SELECT count() FROM batch_trigger')
        count = result.result_rows[0][0]
        if count > 0:
            client.command('TRUNCATE TABLE batch_trigger')
            client.close()
            return True
        client.close()
    except Exception:
        pass
    return False


def check_auto_schedule():
    try:
        client = get_ch_client()
        result = client.query('SELECT max(executed_at) FROM reprocess_log')
        client.close()
        last_run = result.result_rows[0][0]
        if last_run is None:
            return True
        now = datetime.datetime.now()
        elapsed = (now - last_run).total_seconds() / 3600
        return elapsed >= AUTO_INTERVAL_HOURS
    except Exception:
        return False


# ══════════════════════════════════════════════════
# BOUCLE PRINCIPALE (pas de SparkSession ici)
# ══════════════════════════════════════════════════
print('Batch Watcher demarre')
print(f'  Intervalle auto : {AUTO_INTERVAL_HOURS}h')
print(f'  Verification    : toutes les {POLL_INTERVAL_SECONDS}s')
print('  Pas de SparkSession permanente (cores libres pour le streaming)')
print('')

while True:
    if check_manual_trigger():
        print('\n>>> MANUEL : bouton dashboard clique')
        run_batch_reprocessing('Manuel (bouton dashboard)')

    elif check_auto_schedule():
        print(f'\n>>> AUTO : {AUTO_INTERVAL_HOURS}h ecoulees')
        run_batch_reprocessing(f'Automatique ({AUTO_INTERVAL_HOURS}h)')

    time.sleep(POLL_INTERVAL_SECONDS)

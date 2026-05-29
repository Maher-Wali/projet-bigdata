"""
Kafka Producer : envoie les trajets de Decembre dans Kafka
Equivalent au notebook 03 cellule 3
Simule un flux temps reel (~4000 messages/seconde par rafales)
"""
import sys, os
sys.path.insert(0, '/usr/local/spark/python/lib/pyspark.zip')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip')
os.environ['SPARK_HOME'] = '/usr/local/spark'

import subprocess
subprocess.check_call(['pip', 'install', 'kafka-python'])

from kafka import KafkaProducer
import sys, json, time, csv, datetime

KAFKA_BROKER = 'kafka:29092'
TOPIC = 'nyc-taxi-trips'
CSV_PATH = '/home/jovyan/data/fact_trips_sample.csv'
BATCH_SIZE = 20        # Envoyer par rafales de 20 messages
LOG_EVERY = 200        # Log toutes les 200 messages
DELAY = 1.0            # 1s de pause entre chaque rafale → ~20 msg/s (~74 min pour tout envoyer)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=32768,       # 32KB par batch Kafka (plus gros = plus rapide)
    linger_ms=5,            # Attend 5ms pour grouper les messages
    buffer_memory=67108864  # 64MB buffer
)
print(f'Producer connecte a {KAFKA_BROKER}')
print(f'Mode rafale : {BATCH_SIZE} messages/rafale, pause {DELAY}s entre rafales')

count = 0
cycle = 0

# Boucle infinie : relit le CSV quand il est fini (simule un flux continu)
while True:
    cycle += 1
    print(f'\n=== Cycle {cycle} ===')
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_id = float(row['data_id']) if row['data_id'] else None

            # Seulement Decembre (date_id >= 336)
            if date_id is None or date_id < 336:
                continue

            message = {
                'date_id': date_id,
                'pickup_location_id': int(row['localizacao_partida_id']),
                'dropoff_location_id': int(row['localizacao_chegada_id']),
                'weather_id': float(row['clima_id']) if row['clima_id'] else None,
                'trip_distance': float(row['distancia_viagem']),
                'total_amount': float(row['valor_total']),
                'event_time': datetime.datetime.now().isoformat()
            }
            producer.send(TOPIC, value=message)
            count += 1

            if count % LOG_EVERY == 0:
                print(f'  {count} messages envoyes...')
                sys.stdout.flush()

            # Pause entre les rafales
            if count % BATCH_SIZE == 0:
                producer.flush()
                time.sleep(DELAY)

    print(f'Cycle {cycle} termine ({count} messages au total). Reboucle...')
    sys.stdout.flush()

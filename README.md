# NYC Taxi - Big Data Pipeline

Pipeline Big Data pour l'analyse des courses de taxi a New York, utilisant une architecture **Medallion** (Bronze / Silver / Gold) avec HDFS, Apache Spark et Jupyter.

## Architecture

```
                CSV (Bronze)
                    |
            [ HDFS - Namenode/Datanode ]
                    |
        +-----------+-----------+
        |           |           |
   01_bronze    02_silver    03_streaming
   _to_silver   _to_gold
        |           |           |
    Parquet      Parquet     In-memory
    (Silver)     (Gold)      (live stats)
```

### Services Docker

| Service | Port | Role |
|---------|------|------|
| **Namenode** | `9870` (Web UI), `9000` (RPC) | Maitre HDFS - gere les metadonnees |
| **Datanode** | - | Stockage des blocs de donnees HDFS |
| **Spark Master** | `8080` (Web UI), `7077` (RPC) | Orchestre les jobs Spark |
| **Spark Worker** | - | Execute les taches Spark (4GB RAM, 2 cores) |
| **Jupyter** | `8888` | Interface notebook PySpark |

## Donnees

4 fichiers CSV a placer dans `data/` (non inclus dans le repo) :

| Fichier | Description | Taille |
|---------|-------------|--------|
| `fact_trips_sample.csv` | 1M courses de taxi | ~25 MB |
| `dim_data.csv` | Dimension dates (366 jours) | ~10 KB |
| `dim_localizacao.csv` | Dimension localisations (263 zones) | ~10 KB |
| `dim_clima.csv` | Dimension meteo (38 entrees) | ~1 KB |

## Notebooks

### 01 - Bronze to Silver
- Lecture des CSV bruts depuis HDFS
- Traduction des colonnes (portugais -> anglais)
- Nettoyage : suppression des doublons, filtrage des valeurs aberrantes, gestion des nulls
- Ecriture en format Parquet sur HDFS

### 02 - Silver to Gold
5 agregations metier :
1. **Stats par borough** : nb trajets, distance moyenne, tarif moyen, revenu total
2. **Stats mensuelles** : evolution mensuelle des trajets et revenus
3. **Top 10 zones** : zones de pickup les plus frequentees
4. **Impact meteo** : correlation entre meteo et activite taxi
5. **Matrice OD** : top 20 des paires origine-destination

### 03 - Streaming
- Simulation de flux temps reel (micro-batches de 500 lignes)
- Spark Structured Streaming
- Agregation en temps reel par borough (nb trajets, revenu, distance)

## Comment lancer le projet

### Pre-requis
- Docker et Docker Compose installes
- Les 4 fichiers CSV dans le dossier `data/`

### 1. Demarrer les containers

```bash
docker-compose up -d
```

### 2. Charger les donnees dans HDFS

```bash
# Creer les dossiers dans HDFS
docker exec namenode hdfs dfs -mkdir -p /projet/bronze

# Charger les CSV
docker exec namenode hdfs dfs -put /data/fact_trips_sample.csv /projet/bronze/
docker exec namenode hdfs dfs -put /data/dim_data.csv /projet/bronze/
docker exec namenode hdfs dfs -put /data/dim_localizacao.csv /projet/bronze/
docker exec namenode hdfs dfs -put /data/dim_clima.csv /projet/bronze/

# Ouvrir les permissions
docker exec namenode hdfs dfs -chmod -R 777 /projet/
```

### 3. Ouvrir Jupyter

Recuperer le token :
```bash
docker logs jupyter 2>&1 | grep "token="
```

Ouvrir dans le navigateur :
```
http://127.0.0.1:8888/lab?token=<VOTRE_TOKEN>
```

### 4. Executer les notebooks dans l'ordre

1. `01_bronze_to_silver.ipynb` - nettoie les donnees
2. `02_silver_to_gold.ipynb` - cree les agregations
3. `03_streaming.ipynb` - simule le streaming temps reel

### Interfaces web

- HDFS : http://localhost:9870
- Spark Master : http://localhost:8080

### Arreter le projet

```bash
docker-compose down
```

## Stack technique

- **Stockage** : HDFS (Hadoop 3.2.1)
- **Traitement** : Apache Spark 3.5.0 (PySpark)
- **Interface** : Jupyter Notebook
- **Conteneurisation** : Docker Compose

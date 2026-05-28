#!/bin/bash
# ═══════════════════════════════════════════════════════════
# NYC Taxi - Lambda Architecture Pipeline
# ═══════════════════════════════════════════════════════════
# Une seule commande pour tout lancer :
#   ./run_all.sh
#
# Le dashboard sera visible sur : http://localhost:8501
# ═══════════════════════════════════════════════════════════

set -e
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "        NYC Taxi - Architecture Lambda Pipeline"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── Verification des containers ──
echo -e "${CYAN}[0/7] Verification des services Docker...${NC}"
REQUIRED="namenode datanode spark-master spark-worker-1 spark-worker-2 kafka zookeeper jupyter clickhouse dashboard"
ALL_OK=true
for c in $REQUIRED; do
    STATUS=$(docker inspect -f '{{.State.Running}}' $c 2>/dev/null || echo "false")
    if [ "$STATUS" != "true" ]; then
        echo -e "${RED}  ✗ $c n'est pas demarre${NC}"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    echo -e "${YELLOW}  → Demarrage des containers...${NC}"
    docker compose up -d
    echo "  Attente 20s pour que tous les services demarrent..."
    sleep 20
fi
echo -e "${GREEN}  ✓ Tous les services sont prets${NC}"

# Tuer les anciens kernels Jupyter et scripts (liberer les cores Spark)
echo "  Nettoyage des anciens processus..."
docker exec jupyter pkill -f "ipykernel_launcher" 2>/dev/null
docker exec jupyter pkill -f "scripts/" 2>/dev/null
sleep 3

# ── Etape 1 : Charger les CSV dans HDFS (Bronze) ──
echo ""
echo -e "${CYAN}[1/7] Chargement des donnees dans HDFS (Bronze)...${NC}"

docker exec namenode hdfs dfs -mkdir -p /projet/bronze/

# Verifier si les donnees sont deja la
HDFS_CHECK=$(docker exec namenode hdfs dfs -ls /projet/bronze/ 2>/dev/null | wc -l)
if [ "$HDFS_CHECK" -lt 5 ]; then
    docker exec namenode hdfs dfs -put -f /data/fact_trips_sample.csv /projet/bronze/
    docker exec namenode hdfs dfs -put -f /data/dim_data.csv /projet/bronze/
    docker exec namenode hdfs dfs -put -f /data/dim_localizacao.csv /projet/bronze/
    docker exec namenode hdfs dfs -put -f /data/dim_clima.csv /projet/bronze/
    echo -e "${GREEN}  ✓ 4 fichiers charges dans HDFS /projet/bronze/${NC}"
else
    echo -e "${GREEN}  ✓ Donnees deja presentes dans HDFS${NC}"
fi

# ── Etape 2 : Batch - Bronze to Silver (mois 1-11) ──
echo ""
echo -e "${CYAN}[2/7] Batch : Bronze → Silver (nettoyage, mois 1-11)...${NC}"
echo "  Notebook 01 en cours d'execution..."
docker exec jupyter jupyter nbconvert \
    --to notebook --execute \
    --ExecutePreprocessor.timeout=600 \
    --output /tmp/01_output.ipynb \
    /home/jovyan/work/01_bronze_to_silver.ipynb 2>&1 | tail -1
echo -e "${GREEN}  ✓ Donnees Silver pretes dans HDFS${NC}"

# ── Etape 3 : Batch - Silver to Gold + ClickHouse ──
echo ""
echo -e "${CYAN}[3/7] Batch : Silver → Gold (5 agregations + ClickHouse)...${NC}"
echo "  Notebook 02 en cours d'execution..."
docker exec jupyter jupyter nbconvert \
    --to notebook --execute \
    --ExecutePreprocessor.timeout=600 \
    --output /tmp/02_output.ipynb \
    /home/jovyan/work/02_silver_to_gold.ipynb 2>&1 | tail -1
echo -e "${GREEN}  ✓ Agregations batch dans ClickHouse${NC}"

# ── Etape 4 : Batch - ML + Agregations innovantes ──
echo ""
echo -e "${CYAN}[4/7] Batch : SparkML (K-Means, Anomalies, Regression)...${NC}"
echo "  Notebook 02b en cours d'execution..."
docker exec jupyter jupyter nbconvert \
    --to notebook --execute \
    --ExecutePreprocessor.timeout=900 \
    --output /tmp/02b_output.ipynb \
    /home/jovyan/work/02b_ml_batch.ipynb 2>&1 | tail -1
echo -e "${GREEN}  ✓ Resultats ML dans ClickHouse${NC}"

# ── Etape 5 : Lancer le producer Kafka (Decembre) ──
echo ""
echo -e "${CYAN}[5/7] Streaming : Producer Kafka (donnees Decembre)...${NC}"

# Nettoyer les anciens checkpoints streaming (eviter conflits)
docker exec namenode hdfs dfs -rm -r -f /projet/streaming/_checkpoint_december 2>/dev/null || true
docker exec namenode hdfs dfs -rm -r -f /projet/streaming/december_trips 2>/dev/null || true

# Lancer le producer en premier pour que les messages soient prets
docker exec -d jupyter python /home/jovyan/work/scripts/kafka_producer.py
echo "  Producer lance (~200 msg/s, ~7 minutes pour 89K messages)"
sleep 5
echo -e "${GREEN}  ✓ Producer demarre${NC}"

# ── Etape 6 : Demarrer le streaming all-in-one ──
echo ""
echo -e "${CYAN}[6/7] Streaming : Consumer all-in-one (4 streams)...${NC}"
echo "  Stream 1: Stats borough → ClickHouse (2s)"
echo "  Stream 2: Top zones → ClickHouse (3s)"
echo "  Stream 3: Anomalies → ClickHouse (3s)"
echo "  Stream 4: Archivage → HDFS (5s)"

docker exec -d jupyter python /home/jovyan/work/scripts/streaming_all.py
echo "  Attente 40s pour l'initialisation Spark Streaming..."
sleep 40
echo -e "${GREEN}  ✓ 4 streams actifs - donnees en temps reel${NC}"

# ── Etape 7 : Batch Watcher (manuel + automatique toutes les 4h) ──
echo ""
echo -e "${CYAN}[7/7] Batch Watcher : surveillance des declenchements...${NC}"

# Lancer le watcher en arriere-plan (attend les triggers du dashboard + schedule auto 4h)
docker exec -d jupyter python /home/jovyan/work/scripts/batch_watcher.py
echo -e "${GREEN}  ✓ Watcher actif (bouton dashboard + auto toutes les 4h)${NC}"

# ── Resume ──
echo ""
echo "═══════════════════════════════════════════════════════════"
echo -e "${GREEN}  PIPELINE LANCE AVEC SUCCES !${NC}"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  Ouvrez le dashboard : http://localhost:8501"
echo ""
echo "  Ce qui tourne en ce moment :"
echo "    • Batch historique : resultats Jan-Nov dans ClickHouse"
echo "    • Streaming        : donnees Decembre arrivent en live (~5 min)"
echo "    • Batch Watcher    : attend le bouton du dashboard ou auto 4h"
echo "    • Dashboard        : auto-refresh toutes les 3 secondes"
echo ""
echo "  Comment ca marche :"
echo "    1. Le streaming envoie les donnees de Decembre en temps reel"
echo "       → visible immediatement dans l'onglet 'Streaming vs Batch'"
echo "    2. Quand vous voulez un calcul exact, cliquez le bouton"
echo "       'Lancer le Batch' dans l'onglet 'Controle Batch'"
echo "    3. Le batch reprocessing se lance aussi automatiquement"
echo "       toutes les 4 heures"
echo ""
echo "  Interfaces web :"
echo "    • Dashboard  : http://localhost:8501"
echo "    • HDFS       : http://localhost:9870"
echo "    • Spark UI   : http://localhost:8080"
echo "    • Jupyter    : http://localhost:8888"
echo "    • ClickHouse : http://localhost:8123"
echo ""
echo "  Pour tout arreter :"
echo "    docker exec jupyter pkill -f streaming_all.py"
echo "    docker exec jupyter pkill -f kafka_producer.py"
echo "    docker exec jupyter pkill -f batch_watcher.py"
echo ""

-- =============================================
-- NYC Taxi - ClickHouse Schema
-- Base OLAP colonnaire pour batch + streaming
-- =============================================

-- =============================================
-- TABLES BATCH (resultats Silver to Gold)
-- =============================================

CREATE TABLE IF NOT EXISTS batch_stats_borough (
    borough String,
    nb_trips UInt64,
    avg_distance Float64,
    avg_fare Float64,
    total_revenue Float64
) ENGINE = ReplacingMergeTree()
ORDER BY borough;

CREATE TABLE IF NOT EXISTS batch_stats_monthly (
    month UInt8,
    year UInt16,
    nb_trips UInt64,
    total_revenue Float64,
    avg_distance Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS batch_top_zones (
    zone String,
    borough String,
    nb_trips UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (zone, borough);

CREATE TABLE IF NOT EXISTS batch_stats_weather (
    weather_id UInt32,
    temp_max Float64,
    precipitation Float64,
    snow Float64,
    nb_trips UInt64,
    avg_distance Float64,
    avg_fare Float64
) ENGINE = ReplacingMergeTree()
ORDER BY weather_id;

CREATE TABLE IF NOT EXISTS batch_od_matrix (
    zone_pickup String,
    zone_dropoff String,
    nb_trips UInt64,
    avg_fare Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (zone_pickup, zone_dropoff);

-- =============================================
-- TABLES BATCH ML (notebook 02b)
-- =============================================

CREATE TABLE IF NOT EXISTS batch_day_patterns (
    day_of_week UInt8,
    borough String,
    nb_trips UInt64,
    avg_fare Float64,
    avg_distance Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (day_of_week, borough);

CREATE TABLE IF NOT EXISTS batch_clusters (
    cluster_id UInt8,
    center_distance Float64,
    center_fare Float64,
    nb_trips UInt64,
    label String
) ENGINE = ReplacingMergeTree()
ORDER BY cluster_id;

CREATE TABLE IF NOT EXISTS batch_anomalies (
    date_id UInt32,
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    trip_distance Float64,
    total_amount Float64,
    anomaly_score Float64,
    borough String
) ENGINE = MergeTree()
ORDER BY (borough, anomaly_score);

CREATE TABLE IF NOT EXISTS batch_fare_predictions (
    borough String,
    avg_actual_fare Float64,
    avg_predicted_fare Float64,
    r2_score Float64,
    rmse Float64
) ENGINE = ReplacingMergeTree()
ORDER BY borough;

-- =============================================
-- TABLES STREAMING (resultats temps reel)
-- =============================================

CREATE TABLE IF NOT EXISTS stream_borough_stats (
    borough String,
    nb_trips UInt64,
    total_revenue Float64,
    avg_distance Float64,
    avg_fare Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY borough;

CREATE TABLE IF NOT EXISTS stream_top_zones (
    zone String,
    borough String,
    nb_trips UInt64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (zone, borough);

CREATE TABLE IF NOT EXISTS stream_alerts (
    alert_time DateTime DEFAULT now(),
    alert_type String,
    borough String,
    message String,
    current_value Float64,
    historical_value Float64,
    deviation_pct Float64
) ENGINE = MergeTree()
ORDER BY (alert_time, borough);

-- =============================================
-- TABLES BATCH REPROCESSING (notebook 06)
-- Resultats du retraitement periodique des
-- donnees streaming accumulees dans HDFS
-- =============================================

CREATE TABLE IF NOT EXISTS reprocess_borough_stats (
    borough String,
    nb_trips UInt64,
    total_revenue Float64,
    avg_distance Float64,
    avg_fare Float64
) ENGINE = ReplacingMergeTree()
ORDER BY borough;

CREATE TABLE IF NOT EXISTS reprocess_top_zones (
    zone String,
    borough String,
    nb_trips UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (zone, borough);

CREATE TABLE IF NOT EXISTS reprocess_daily_stats (
    day UInt8,
    nb_trips UInt64,
    total_revenue Float64,
    avg_fare Float64
) ENGINE = ReplacingMergeTree()
ORDER BY day;

CREATE TABLE IF NOT EXISTS reprocess_log (
    cycle_num UInt32,
    executed_at DateTime,
    rows_processed UInt64,
    duration_seconds Float64,
    boroughs_found UInt8,
    zones_found UInt8
) ENGINE = MergeTree()
ORDER BY cycle_num;

-- =============================================
-- TABLE TRIGGER (declenchement batch depuis dashboard)
-- Le dashboard insere une ligne pour demander
-- un batch reprocessing, le watcher la detecte
-- =============================================

CREATE TABLE IF NOT EXISTS batch_trigger (
    requested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY requested_at;

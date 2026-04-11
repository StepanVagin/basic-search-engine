#!/bin/bash
# store_index.sh - Create Cassandra tables and load index data from HDFS.

echo "=== Storing index data from HDFS to Cassandra ==="

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)

# Unset so executors use the archived venv Python
unset PYSPARK_PYTHON

# --- Step 1: Create Cassandra keyspace and tables ---
echo "--- Creating Cassandra keyspace and tables ---"
python3 << 'PYEOF'
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term TEXT,
        doc_id TEXT,
        tf INT,
        PRIMARY KEY (term, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term TEXT PRIMARY KEY,
        df INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id TEXT PRIMARY KEY,
        doc_length INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS corpus_stats (
        id INT PRIMARY KEY,
        total_docs INT,
        avg_doc_length DOUBLE
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_meta (
        doc_id TEXT PRIMARY KEY,
        title TEXT
    )
""")

print("Cassandra keyspace and tables created successfully.")
cluster.shutdown()
PYEOF

if [ $? -ne 0 ]; then echo "Table creation FAILED"; exit 1; fi

# --- Step 2: Load data using PySpark with Spark Cassandra Connector ---
echo "--- Loading data into Cassandra via PySpark ---"
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
    --conf spark.sql.catalog.cassandra=com.datastax.spark.connector.datasource.CassandraCatalog \
    store_index.py

if [ $? -ne 0 ]; then echo "Data loading FAILED"; exit 1; fi

echo "=== Index data stored in Cassandra successfully ==="

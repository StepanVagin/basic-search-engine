#!/bin/bash
# search.sh - Run BM25 search on YARN cluster.
# Usage: bash search.sh "your search query here"

if [ -z "$1" ]; then
    echo "Usage: bash search.sh \"<query>\""
    exit 1
fi

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Python of the executor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
    --conf spark.sql.catalog.cassandra=com.datastax.spark.connector.datasource.CassandraCatalog \
    --archives /app/.venv.tar.gz#.venv \
    query.py "$@"

#!/bin/bash
# search.sh - Run BM25 search on YARN cluster.
# Usage: bash search.sh "your search query here"

if [ -z "$1" ]; then
    echo "Usage: bash search.sh \"<query>\""
    exit 1
fi

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 512m \
    --num-executors 1 \
    --archives /app/.venv.tar.gz#.venv \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    query.py "$@"

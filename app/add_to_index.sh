#!/bin/bash
# add_to_index.sh - Add a single document from the local filesystem to the index.
# Usage: bash add_to_index.sh <path_to_file>
#
# The filename must follow the convention: <doc_id>_<Title_Words>.txt
# Example: 99999_My_New_Article.txt

FILE_PATH=$1

if [ -z "$FILE_PATH" ]; then
    echo "Usage: bash add_to_index.sh <path_to_file>"
    exit 1
fi

if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File not found: $FILE_PATH"
    exit 1
fi

echo "=== Adding document to index: $FILE_PATH ==="

# Stage the file in a temporary HDFS directory
hdfs dfs -rm -r -f /tmp/add_doc
hdfs dfs -mkdir -p /tmp/add_doc
hdfs dfs -put "$FILE_PATH" /tmp/add_doc/

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# Run PySpark to tokenize, compute TF, and update all Cassandra tables
spark-submit \
    --master yarn \
    --deploy-mode client \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
    --conf spark.sql.catalog.cassandra=com.datastax.spark.connector.datasource.CassandraCatalog \
    add_to_index.py

EXIT_CODE=$?

# Clean up temp HDFS directory
hdfs dfs -rm -r -f /tmp/add_doc

if [ $EXIT_CODE -ne 0 ]; then
    echo "Failed to add document to index."
    exit 1
fi

echo "=== Document added to index successfully ==="

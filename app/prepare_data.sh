#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD e.parquet or any parquet file before you run this

if hdfs dfs -test -e /data; then
    echo "Data already in HDFS, skipping prepare step."
else
    hdfs dfs -put -f e.parquet / && \
        spark-submit --driver-memory 512m prepare_data.py && \
        echo "Putting data to hdfs" && \
        hdfs dfs -put data / && \
        hdfs dfs -ls /data && \
        hdfs dfs -ls /indexer/data && \
        echo "done data preparation!"
fi

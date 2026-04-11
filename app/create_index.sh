#!/bin/bash
# create_index.sh - Run all MapReduce pipelines to build the search index.
# Usage: bash create_index.sh [input_path]
#   input_path: HDFS path to input documents (default: /input/data)

INPUT_PATH=${1:-/input/data}
STREAMING_JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

echo "=== Creating index from: $INPUT_PATH ==="

# Clean up previous index data
hdfs dfs -rm -r -f /indexer

# ---------------------------------------------------------------
# Pipeline 1: Build inverted index (term, doc_id, tf)
# ---------------------------------------------------------------
echo "--- Pipeline 1: Building inverted index ---"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -input "$INPUT_PATH" \
    -output /indexer/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

if [ $? -ne 0 ]; then echo "Pipeline 1 FAILED"; exit 1; fi

# ---------------------------------------------------------------
# Pipeline 2: Build vocabulary with document frequencies (term, df)
# ---------------------------------------------------------------
echo "--- Pipeline 2: Building vocabulary ---"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -input /indexer/index \
    -output /indexer/vocabulary \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py

if [ $? -ne 0 ]; then echo "Pipeline 2 FAILED"; exit 1; fi

# ---------------------------------------------------------------
# Pipeline 3: Build document statistics (doc_id, doc_length)
# ---------------------------------------------------------------
echo "--- Pipeline 3: Building document statistics ---"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -input /indexer/index \
    -output /indexer/doc_stats \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -file mapreduce/mapper3.py \
    -file mapreduce/reducer3.py

if [ $? -ne 0 ]; then echo "Pipeline 3 FAILED"; exit 1; fi

echo "=== Index created successfully ==="
hdfs dfs -ls /indexer/
hdfs dfs -ls /indexer/index
hdfs dfs -ls /indexer/vocabulary
hdfs dfs -ls /indexer/doc_stats

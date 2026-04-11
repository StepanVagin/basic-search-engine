#!/bin/bash
# index.sh - Run the full indexing pipeline: MapReduce → Cassandra.
# Usage: bash index.sh [input_path]

INPUT_PATH=${1:-/input/data}

echo "=== Running full indexing pipeline ==="

# Step 1: Create index using Hadoop MapReduce pipelines
bash create_index.sh "$INPUT_PATH"
if [ $? -ne 0 ]; then echo "Index creation failed"; exit 1; fi

# Step 2: Store index data in Cassandra
bash store_index.sh
if [ $? -ne 0 ]; then echo "Index storage failed"; exit 1; fi

echo "=== Full indexing pipeline complete ==="

#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment (reuse if already built)
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env (reuse if already built)
if [ ! -f ".venv.tar.gz" ]; then
  venv-pack -o .venv.tar.gz
fi

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "English breakfast"

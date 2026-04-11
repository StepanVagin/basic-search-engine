#!/bin/bash
# This will run only by the master node

echo "cluster-slave-1" > "$HADOOP_HOME/etc/hadoop/workers"

# Allow SSH to cluster nodes without host-key prompt
mkdir -p /root/.ssh
ssh-keyscan -T 5 -p 22 -H cluster-slave-1 >> /root/.ssh/known_hosts 2>/dev/null
ssh-keyscan -T 5 -p 2122 -H cluster-slave-1 >> /root/.ssh/known_hosts 2>/dev/null

# Skip host key checking for Hadoop start scripts (covers master SSHing to itself)
export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"

# Format NameNode on first run (no current/VERSION means unformatted)
if [ ! -f "/usr/local/hadoop/hdfs/namenode/current/VERSION" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting Yarn daemons
$HADOOP_HOME/sbin/start-yarn.sh
# yarn --daemon start resourcemanager

# Start mapreduce history server
mapred --daemon start historyserver


# track process IDs of services
jps -lm

# Wait for NameNode to finish initializing
echo "Waiting for NameNode on port 9000..."
until hdfs dfsadmin -safemode get &>/dev/null; do
  sleep 3
done
echo "NameNode is ready."

# Wait for at least 1 live DataNode before writing to HDFS
echo "Waiting for DataNode to register (may take a while under emulation)..."
ATTEMPTS=0
until hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; do
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ $ATTEMPTS -ge 60 ]; then
    echo "WARNING: DataNode not registered after 3 minutes, proceeding anyway."
    break
  fi
  sleep 3
done
echo "DataNode check done. Current report:"
hdfs dfsadmin -report 2>/dev/null | head -5

# If namenode in safemode then leave it
hdfs dfsadmin -safemode leave

# create a directory for spark apps in HDFS
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars


# Copy all jars to HDFS (skip if already uploaded)
if ! hdfs dfs -test -e /apps/spark/jars/spark-core_2.12*.jar; then
  hdfs dfs -put /usr/local/spark/jars/* /apps/spark/jars/
  hdfs dfs -chmod +rx /apps/spark/jars/
else
  echo "Spark jars already in HDFS, skipping upload."
fi


# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root

echo "done starting services"


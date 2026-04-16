"""Load index data from HDFS into Cassandra using PySpark + Spark Cassandra Connector."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Store Index to Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.output.batch.size.rows", "100") \
    .config("spark.cassandra.output.concurrent.writes", "2") \
    .getOrCreate()

sc = spark.sparkContext

# --- 1. Inverted index: term, doc_id, tf ---
print("Loading inverted index...")
index_rdd = sc.textFile("/indexer/index") \
    .map(lambda line: line.split('\t')) \
    .filter(lambda p: len(p) == 3)
index_df = index_rdd.toDF(["term", "doc_id", "tf"]) \
    .withColumn("tf", F.col("tf").cast(IntegerType()))

index_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="inverted_index", keyspace="search_engine") \
    .mode("append") \
    .save()
print(f"  Wrote {index_df.count()} inverted index entries.")

# --- 2. Vocabulary: term, df ---
print("Loading vocabulary...")
vocab_rdd = sc.textFile("/indexer/vocabulary") \
    .map(lambda line: line.split('\t')) \
    .filter(lambda p: len(p) == 2)
vocab_df = vocab_rdd.toDF(["term", "df"]) \
    .withColumn("df", F.col("df").cast(IntegerType()))

vocab_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="search_engine") \
    .mode("append") \
    .save()
print(f"  Wrote {vocab_df.count()} vocabulary entries.")

# --- 3. Document statistics: doc_id, doc_length ---
print("Loading document statistics...")
doc_stats_rdd = sc.textFile("/indexer/doc_stats") \
    .map(lambda line: line.split('\t')) \
    .filter(lambda p: len(p) == 2)
doc_stats_df = doc_stats_rdd.toDF(["doc_id", "doc_length"]) \
    .withColumn("doc_length", F.col("doc_length").cast(IntegerType()))

doc_stats_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_stats", keyspace="search_engine") \
    .mode("append") \
    .save()
print(f"  Wrote {doc_stats_df.count()} document stat entries.")

# --- 4. Corpus-level statistics: total_docs, avg_doc_length ---
print("Computing and storing corpus statistics...")
total_docs = doc_stats_df.count()
avg_doc_length = doc_stats_df.agg(F.avg("doc_length")).collect()[0][0]

corpus_stats_df = spark.createDataFrame(
    [(1, total_docs, float(avg_doc_length))],
    StructType([
        StructField("id", IntegerType(), False),
        StructField("total_docs", IntegerType(), False),
        StructField("avg_doc_length", DoubleType(), False),
    ])
)
corpus_stats_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_stats", keyspace="search_engine") \
    .mode("append") \
    .save()
print(f"  total_docs={total_docs}, avg_doc_length={avg_doc_length:.2f}")

# --- 5. Document metadata: doc_id, title ---
print("Loading document metadata...")
doc_meta_rdd = sc.textFile("/input/data") \
    .map(lambda line: line.split('\t', 2)) \
    .filter(lambda p: len(p) >= 2)
doc_meta_df = doc_meta_rdd.toDF(["doc_id", "title", "text"]) \
    .select("doc_id", F.regexp_replace(F.col("title"), "_", " ").alias("title"))

doc_meta_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_meta", keyspace="search_engine") \
    .mode("append") \
    .save()
print(f"  Wrote {doc_meta_df.count()} document metadata entries.")

print("All index data stored in Cassandra successfully!")
spark.stop()

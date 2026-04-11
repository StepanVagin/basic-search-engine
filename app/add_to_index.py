"""Index a new document and incrementally update all Cassandra tables.

Reads the raw file from /tmp/add_doc/ in HDFS, tokenizes using PySpark RDD,
and merges the new postings/stats into the existing Cassandra index.
"""
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Add Document to Index") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

sc = spark.sparkContext

# ------------------------------------------------------------------
# 1. Read and parse the document from HDFS
# ------------------------------------------------------------------
doc_rdd = sc.wholeTextFiles("/tmp/add_doc/")


def parse_doc(path_content):
    path, content = path_content
    name = os.path.basename(path).replace(".txt", "")
    parts = name.split("_", 1)
    doc_id = parts[0]
    title = parts[1].replace("_", " ") if len(parts) > 1 else ""
    return (doc_id, title, content)


docs = doc_rdd.map(parse_doc).collect()
if not docs:
    print("Error: no document found in /tmp/add_doc/")
    raise SystemExit(1)

doc_id, title, text = docs[0]
print(f"Indexing document: id={doc_id}, title='{title}'")

# ------------------------------------------------------------------
# 2. Tokenize and compute term frequencies using RDD API
# ------------------------------------------------------------------
content = title + " " + text
tokens_rdd = sc.parallelize(re.findall(r"[a-z0-9]{2,}", content.lower()))

tf_rdd = tokens_rdd \
    .map(lambda t: (t, 1)) \
    .reduceByKey(lambda a, b: a + b)
# tf_rdd: [(term, tf), ...]

tf_data = tf_rdd.collect()
doc_length = sum(tf for _, tf in tf_data)

print(f"  Unique terms: {len(tf_data)}, Document length: {doc_length}")

# ------------------------------------------------------------------
# 3. Update inverted_index (simple upsert)
# ------------------------------------------------------------------
index_rows = [(term, doc_id, int(tf)) for term, tf in tf_data]
index_df = spark.createDataFrame(index_rows, ["term", "doc_id", "tf"])

index_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="inverted_index", keyspace="search_engine") \
    .mode("append") \
    .save()

# ------------------------------------------------------------------
# 4. Update vocabulary (increment df for terms in new doc)
# ------------------------------------------------------------------
old_vocab = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="search_engine") \
    .load()

new_terms_df = spark.createDataFrame(
    [(term, 1) for term, _ in tf_data],
    ["term", "new_df"],
)

merged_vocab = old_vocab.join(new_terms_df, "term", "full_outer") \
    .select(
        F.col("term"),
        (F.coalesce(F.col("df"), F.lit(0)) + F.coalesce(F.col("new_df"), F.lit(0)))
        .alias("df"),
    )

merged_vocab.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="search_engine") \
    .mode("append") \
    .save()

# ------------------------------------------------------------------
# 5. Insert doc_stats for the new document
# ------------------------------------------------------------------
doc_stats_df = spark.createDataFrame(
    [(doc_id, doc_length)],
    StructType([
        StructField("doc_id", StringType()),
        StructField("doc_length", IntegerType()),
    ]),
)

doc_stats_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_stats", keyspace="search_engine") \
    .mode("append") \
    .save()

# ------------------------------------------------------------------
# 6. Update corpus_stats (total_docs + 1, recompute avg_doc_length)
# ------------------------------------------------------------------
old_corpus = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_stats", keyspace="search_engine") \
    .load().collect()[0]

old_total = old_corpus["total_docs"]
old_avg = old_corpus["avg_doc_length"]
new_total = old_total + 1
new_avg = (old_avg * old_total + doc_length) / new_total

corpus_df = spark.createDataFrame(
    [(1, new_total, new_avg)],
    StructType([
        StructField("id", IntegerType()),
        StructField("total_docs", IntegerType()),
        StructField("avg_doc_length", DoubleType()),
    ]),
)

corpus_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_stats", keyspace="search_engine") \
    .mode("append") \
    .save()

# ------------------------------------------------------------------
# 7. Insert doc_meta for the new document
# ------------------------------------------------------------------
doc_meta_df = spark.createDataFrame(
    [(doc_id, title)],
    StructType([
        StructField("doc_id", StringType()),
        StructField("title", StringType()),
    ]),
)

doc_meta_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_meta", keyspace="search_engine") \
    .mode("append") \
    .save()

print(f"Document {doc_id} ('{title}') added to index successfully!")
spark.stop()

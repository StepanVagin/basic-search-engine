"""BM25 search engine using PySpark RDD API.

Reads index and vocabulary from Cassandra, computes BM25 scores for a query,
and returns the top 10 most relevant documents.
"""
import sys
import re
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Read query
# ---------------------------------------------------------------------------
query = ' '.join(sys.argv[1:])
if not query:
    query = input("Enter search query: ")

# Tokenize query (same logic as mapper1.py)
query_terms = list(set(re.findall(r'[a-z0-9]{2,}', query.lower())))
if not query_terms:
    print("No valid query terms found.")
    sys.exit(1)

# BM25 hyper-parameters
K1 = 1.0
B = 0.75

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("BM25 Search") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.input.fetch.size_in_rows", "50") \
    .config("spark.cassandra.input.split.size_in_mb", "4") \
    .config("spark.cassandra.concurrent.reads", "2") \
    .getOrCreate()

sc = spark.sparkContext

# ---------------------------------------------------------------------------
# Load data from Cassandra
# ---------------------------------------------------------------------------

# Corpus-level statistics (single row)
corpus_rows = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_stats", keyspace="search_engine") \
    .load().collect()
if not corpus_rows:
    print("ERROR: corpus_stats table is empty — run the indexing pipeline first (bash index.sh).")
    spark.stop()
    sys.exit(1)
corpus_row = corpus_rows[0]

N = corpus_row["total_docs"]
avgdl = corpus_row["avg_doc_length"]

# Vocabulary – only the query terms (predicate pushdown on partition key)
vocab_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="search_engine") \
    .load() \
    .filter(F.col("term").isin(query_terms))

vocab_map = {row["term"]: row["df"] for row in vocab_df.collect()}

# Inverted index – only postings for query terms
index_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="inverted_index", keyspace="search_engine") \
    .load() \
    .filter(F.col("term").isin(query_terms))

# Document statistics (doc_id -> doc_length)
doc_stats_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_stats", keyspace="search_engine") \
    .load()

# Document metadata for displaying results
doc_meta_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_meta", keyspace="search_engine") \
    .load()

title_map = {row["doc_id"]: row["title"] for row in doc_meta_df.collect()}

# ---------------------------------------------------------------------------
# BM25 scoring using RDD API
# ---------------------------------------------------------------------------

# Broadcast small lookup structures to all executors
vocab_bc = sc.broadcast(vocab_map)
N_bc = sc.broadcast(N)
avgdl_bc = sc.broadcast(avgdl)

# index RDD: (doc_id, (term, tf))
index_rdd = index_df.rdd.map(lambda r: (r["doc_id"], (r["term"], r["tf"])))

# doc_stats RDD: (doc_id, doc_length)
stats_rdd = doc_stats_df.rdd.map(lambda r: (r["doc_id"], r["doc_length"]))

# Join on doc_id -> (doc_id, ((term, tf), doc_length))
joined_rdd = index_rdd.join(stats_rdd)


def bm25_score(record):
    """Compute per-term BM25 contribution for one (doc, term) pair."""
    doc_id, ((term, tf), doc_length) = record
    df = vocab_bc.value.get(term, 0)
    if df == 0:
        return (doc_id, 0.0)
    n = N_bc.value
    avg = avgdl_bc.value
    idf = math.log((n - df + 0.5) / (df + 0.5) + 1.0)
    tf_component = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * doc_length / avg))
    return (doc_id, idf * tf_component)


# Sum BM25 scores across query terms per document, then take top 10
top10 = joined_rdd \
    .map(bm25_score) \
    .reduceByKey(lambda a, b: a + b) \
    .takeOrdered(10, key=lambda x: -x[1])

# ---------------------------------------------------------------------------
# Display results
# ---------------------------------------------------------------------------
print(f"\nQuery: '{query}'")
print(f"Terms: {query_terms}")
print("-" * 70)
if not top10:
    print("No matching documents found.")
else:
    for rank, (doc_id, score) in enumerate(top10, 1):
        title = title_map.get(doc_id, "Unknown")
        print(f"{rank:2d}. [Doc {doc_id}] {title}  (BM25: {score:.4f})")
print("-" * 70)

spark.stop()

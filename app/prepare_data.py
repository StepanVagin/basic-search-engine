import os
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.parquet("/e.parquet")
n = 100
df = df.select(['id', 'title', 'text']) \
    .filter(F.col('text').isNotNull() & (F.col('text') != '')) \
    .sample(fraction=100 * n / df.count(), seed=0) \
    .limit(n)


os.makedirs("data", exist_ok=True)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


# Read docs from local data/ and write to HDFS /input/data as one partition
def parse_doc(path_content):
    path, content = path_content
    name = os.path.basename(path).replace(".txt", "")
    parts = name.split("_", 1)
    doc_id = parts[0]
    title = parts[1] if len(parts) > 1 else ""
    return f"{doc_id}\t{title}\t{content}"


sc.wholeTextFiles("file:///app/data/") \
    .map(parse_doc) \
    .coalesce(1) \
    .saveAsTextFile("/input/data")
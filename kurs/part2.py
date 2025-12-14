from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame
import sys

# параметр n
if len(sys.argv) < 3:
    print("Пример: spark-submit part2_graph.py <n> <csv>")
    sys.exit(1)
N = int(sys.argv[1])
filename = sys.argv[2]
if N < 1:
    print("n должно быть >= 1")
    sys.exit(1)

# Spark сессия
spark = (
    SparkSession.builder.appName("user_chains")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.10.0")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setCheckpointDir("/tmp")

# загрузка данных из CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)

print("Строк в файле:", df.count())

# строим граф ответов
edges = df.filter(  # только твиты, которые являются ответом на другой твит
    F.col("tweetid").isNotNull() & F.col("in_reply_to_tweetid").isNotNull()
).select(  # src -> dst
    F.col("in_reply_to_tweetid").cast("string").alias("src"),
    F.col("tweetid").cast("string").alias("dst"),
)

# информация по твитам (вершины графа)
tweets_info = df.select(
    F.col("tweetid").cast("string").alias("id"),
    "userid",
    "tweet_time",
    F.col("in_reply_to_tweetid").cast("string").alias("parent_id"),
    F.col("reply_count").try_cast("long").alias("reply_count"),
)

# вершины = только реальные твиты из файла
vertices = tweets_info.dropDuplicates(["id"])

print("Вершин:", vertices.count())
print("Рёбер:", edges.count())

g = GraphFrame(vertices, edges)

# BFS: ищем цепочки ответов
paths = g.bfs(
    fromExpr="parent_id IS NULL",  
    toExpr="parent_id IS NOT NULL AND reply_count = 0", 
)

if paths.rdd.isEmpty():
    print("Цепочки не найдены по заданным условиям")
    spark.stop()
    sys.exit(0)

# берём только нужные поля
paths_simple = paths.select(
    F.col("from.id").alias("root_id"),
    F.col("from.userid").alias("root_userid"),
    F.col("to.id").alias("tweet_id"),
).dropDuplicates(["root_id", "tweet_id"])

# считаем длину цепочки: сколько ответов + сам первый твит
chains = (
    paths_simple.groupBy("root_id", "root_userid")
    .agg(F.count("*").alias("replies"))
    .withColumn("size", F.col("replies") + 1)
)

# сортировка цепочек по размеру
ordered = chains.orderBy(F.desc("size"))

total = ordered.count()
print("Цепочек найдено:", total)

if total == 0:
    print("Нет подходящих цепочек")
    spark.stop()
    sys.exit(0)

if N > total:
    print(f"Нет такой цепочки (n > {total})")
    spark.stop()
    sys.exit(0)

# n-я по длине цепочка
nth = ordered.limit(N).collect()[N - 1]

print("\nРезультат для n =", N)
print("Длина цепочки:", nth["size"])
print("Первый твит:", nth["root_id"])
print("Автор:", nth["root_userid"])

spark.stop()

# Импорт необходимых библиотек
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from graphframes import GraphFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Создание Spark сессии с GraphFrames
spark = SparkSession.builder \
    .appName("TwitterContinuousChainGraphFrames") \
    .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.10.0") \
    .getOrCreate()

# Загрузка данных Twitter [file:1]
filename = sys.argv[2]
df = spark.read.option("header", "true").csv(filename)

# Парсинг колонки tweettime [file:1]
df = df.withColumn("tweet_time",
    try_to_timestamp(col("tweet_time"))) \
    .filter(col("tweet_time").isNotNull())

# Сортировка по времени и добавление row_id [file:1]
df_sorted = df.orderBy("tweet_time")
df_with_row = df_sorted.withColumn("row_id", monotonically_increasing_id())

# Окно для анализа цепочек по пользователям [file:1]
window_spec = Window.partitionBy("userid").orderBy("tweet_time")

# Подготовка данных для цепочек с prev_tweetid [file:1]
df_chains_prep = df_with_row.withColumn("prev_tweetid", lag("tweetid").over(window_spec)) \
    .withColumn("prev_tweet_time", lag("tweet_time").over(window_spec)) \
    .withColumn("time_diff_minutes",
        when(col("prev_tweet_time").isNull(), 0)
        .otherwise((unix_timestamp("tweet_time") - unix_timestamp("prev_tweet_time")) / 60)) \
    .withColumn("is_continuous", 
        (col("userid") == lag("userid").over(window_spec)) & 
        (col("time_diff_minutes") <= 60))  # Цепочка в пределах 60 минут

# Определяем начало цепочки и номер цепочки [file:1]
df_chains = df_chains_prep.withColumn("chain_start",
    when(col("is_continuous") == False, 1).otherwise(0))
df_chains_final = df_chains.withColumn("chain_number",
    sum("chain_start").over(window_spec))

# Статистика цепочек [file:1]
chain_stats = df_chains_final.groupBy("userid", "chain_number") \
    .agg(count("*").alias("chain_length")) \
    .orderBy(col("chain_length").desc(), col("chain_number").asc())

# Ранжирование всех цепочек [file:1]
window_all = Window.orderBy(col("chain_length").desc(), col("chain_number").asc())
ranked_chains = chain_stats.withColumn("rank", row_number().over(window_all))

# Находим 3-ю по длине цепочку (n=3) [file:1]
n = sys.argv[1]
nth_chain = ranked_chains.filter(col("rank") == n).collect()[0]
nth_userid = nth_chain["userid"]
nth_length = nth_chain["chain_length"]
nth_chain_num = nth_chain["chain_number"]

print(f"N-ая ({n}) по длине непрерывная цепочка имеет длину: {nth_length}")
print(f"Пользователь ID: {nth_userid}")
print(f"Номер цепочки: {nth_chain_num}")

# Показываем твиты из этой цепочки [file:1]
nth_chain_tweets = df_chains_final.filter((col("userid") == nth_userid) & 
                                         (col("chain_number") == nth_chain_num)) \
    .select("tweetid", "tweet_time", "tweet_text") \
    .orderBy("tweet_time")
print("Твиты в n-й цепочке:")
nth_chain_tweets.show(truncate=False)

# === GraphFrames решение === [file:1]
print("\n=== GraphFrames анализ ===")

# Вершины графа - твиты [file:1]
vertices = df_chains_final.select(
    col("tweetid").alias("id"),
    col("userid").alias("user"),
    "tweet_time",
    "tweet_text"
).withColumn("label", lit("tweet"))

# Ребра - непрерывные последовательности твитов одного пользователя [file:1]
edges = df_chains_final.filter(col("is_continuous") == True) \
    .select(
        col("tweetid").alias("src"),
        col("prev_tweetid").alias("dst"),
        col("userid").alias("user"),
        lit("continuous").alias("relationship")
    )

# Создание графа GraphFrames [file:1]
g = GraphFrame(vertices, edges)

# Находим мотивы цепочек длиной 2+ твитов для пользователя nth_userid [file:1]
print(f"GraphFrames: Поиск цепочек пользователя {nth_userid}")
user_motifs = g.find("(a)-[e1]->(b)").filter(col("a.user") == nth_userid)

if user_motifs.count() > 0:
    print("Примеры цепочек из графа для этого пользователя:")
    user_motifs.select("a.id", "a.tweet_time", "b.tweet_text", "b.tweet_time").show(10)
else:
    print("Цепочек длиной 2+ не найдено в графе для этого пользователя")

# Анализ входящей степени (длина цепочек) [file:1]
in_degree = g.inDegrees.filter(col("inDegree") > 0)
print("Твиты с входящими связями (начало цепочек):")
in_degree.orderBy(col("inDegree").desc()).show(10)

print("Анализ завершен. GraphFrames граф построен успешно!")
spark.stop()

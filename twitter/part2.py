# part2.py
# Часть 2. Вариант 2:
# Найти наибольшую компоненту связности социального графа
# для российских пользователей.
#
# Запуск (версию graphframes уточните под свой Spark):
#   spark-submit --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 \
#       part2.py ira_tweets_csv_hashed_head.csv ira_users_csv_hashed_head.csv

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count
from graphframes import GraphFrame

def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit part2.py <tweets_csv> <users_csv>")
        sys.exit(1)

    tweets_path = sys.argv[1]
    users_path = sys.argv[2]

    spark = SparkSession.builder.appName("HW_Part2_Var2").config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.10.0").getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp")

    # Загрузка
    tweets_raw = spark.read.csv(
        tweets_path,
        header=True,
        inferSchema=True
    )

    users_raw = spark.read.csv(
        users_path,
        header=True,
        inferSchema=True
    )

    # Проверка колонок по описанию датасета
    user_required = {"userid", "account_language"}
    tweet_required = {"userid", "in_reply_to_userid", "retweet_userid"}

    miss_users = user_required - set(users_raw.columns)
    miss_tweets = tweet_required - set(tweets_raw.columns)

    if miss_users:
        raise ValueError("Missing columns in users CSV: " + ", ".join(miss_users))
    if miss_tweets:
        raise ValueError("Missing columns in tweets CSV: " + ", ".join(miss_tweets))

    # Приведение типов
    users = users_raw.select(
        col("userid").cast("string").alias("userid"),
        col("account_language").cast("string").alias("account_language")
    )

    tweets = tweets_raw.select(
        col("userid").cast("string").alias("userid"),
        col("in_reply_to_userid").cast("string").alias("in_reply_to_userid"),
        col("retweet_userid").cast("string").alias("retweet_userid")
    )

    ########################################
    # 1. Вершины: российские пользователи
    ########################################

    ru_users = users.filter(col("account_language") == "ru") \
                    .select(col("userid").alias("id")) \
                    .dropDuplicates(["id"])

    vertices = ru_users

    ########################################
    # 2. Рёбра: взаимодействия (reply + retweet) между РФ-пользователями
    ########################################

    edges_reply = tweets.select(
        col("userid").alias("src"),
        col("in_reply_to_userid").alias("dst")
    )

    edges_retweet = tweets.select(
        col("userid").alias("src"),
        col("retweet_userid").alias("dst")
    )

    edges_raw = edges_reply.union(edges_retweet)

    edges_non_null = edges_raw.filter(col("dst").isNotNull())

    # src в ru_users
    edges_src_ru = edges_non_null.join(
        ru_users,
        edges_non_null["src"] == ru_users["id"],
        how="inner"
    ).drop(ru_users["id"])

    # dst в ru_users
    edges_ru_ru = edges_src_ru.join(
        ru_users,
        edges_src_ru["dst"] == ru_users["id"],
        how="inner"
    ).drop(ru_users["id"])

    edges = edges_ru_ru.select("src", "dst").dropDuplicates(["src", "dst"])

    ########################################
    # 3. Граф и компоненты связности
    ########################################

    g = GraphFrame(vertices, edges)

    cc = g.connectedComponents()

    cc_grouped = cc.groupBy("component").agg(
        spark_count("*").alias("size")
    )

    largest_cc = cc_grouped.orderBy(col("size").desc()).limit(1)

    print("=== Largest connected component among RU users ===")
    largest_cc.show(truncate=False)

    component_id = largest_cc.collect()[0]["component"]

    largest_vertices = cc.filter(col("component") == component_id)

    print("=== Vertices in the largest component (userid) ===")
    largest_vertices.select("id").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

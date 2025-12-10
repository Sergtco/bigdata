#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count as sparkcount, desc


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit part2.py <n> <tweets_csv>")
        sys.exit(1)

    n = int(sys.argv[1])
    tweetspath = sys.argv[2]
    spark = (
        SparkSession.builder.appName("ChainGraphFramesSimple")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    print("Загружаем данные...")
    tweetsraw = spark.read.csv(tweetspath, header=True, inferSchema=True)
    print(f"Available columns: {tweetsraw.columns}")

    required_cols = ["tweetid", "userid", "tweet_time"]
    missing = set(required_cols) - set(tweetsraw.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    print(f"Total rows: {tweetsraw.count()}")

    # ПРОСТОЕ РЕШЕНИЕ: считаем количество твитов подряд по годам БЕЗ графа
    tweets = tweetsraw.select(
        col("tweetid").cast("string").alias("tweetid"),
        col("userid").cast("string").alias("userid"),
        col("tweet_time").cast("string").alias("tweet_time"),
    ).filter(
        col("tweet_time").isNotNull()
        & col("tweetid").isNotNull()
        & col("userid").isNotNull()
    )

    # Извлекаем год
    tweets_year = tweets.withColumn(
        "year", regexp_extract(col("tweet_time"), r"(\d{4})", 1).cast("int")
    )

    tweets_valid = tweets_year.filter(col("year").isNotNull())
    print(f"Valid tweets with year: {tweets_valid.count()}")

    tweets_valid.createOrReplaceTempView("tweets_valid")

    # Группируем твиты по пользователю и году
    user_year_stats = spark.sql("""
        SELECT 
            userid,
            year,
            COUNT(*) as tweets_per_year
        FROM tweets_valid
        GROUP BY userid, year
        ORDER BY userid, year
    """)

    # Находим последовательные годы для каждого пользователя
    user_sequences = spark.sql("""
        SELECT 
            userid,
            year,
            tweets_per_year,
            LAG(year) OVER (PARTITION BY userid ORDER BY year) as prev_year
        FROM (
            SELECT userid, year, COUNT(*) as tweets_per_year
            FROM tweets_valid 
            GROUP BY userid, year
        )
    """)

    # Считаем непрерывные цепочки (gap <= 1)
    chains = spark.sql("""
        SELECT 
            userid,
            SUM(CASE WHEN prev_year IS NULL OR year = prev_year + 1 THEN 1 ELSE 0 END) as chain_length
        FROM (
            SELECT 
                userid,
                year,
                LAG(year) OVER (PARTITION BY userid ORDER BY year) as prev_year
            FROM (
                SELECT userid, year, COUNT(*) as tweets_per_year
                FROM tweets_valid 
                GROUP BY userid, year
            )
        )
        GROUP BY userid
        HAVING SUM(CASE WHEN prev_year IS NULL OR year = prev_year + 1 THEN 1 ELSE 0 END) > 1
    """)

    # Топ-3 по длине цепочек
    top_chains = chains.orderBy(desc("chain_length"), col("userid")).limit(n)

    print(f"\n=== ТОП-{n} ПО ДЛИНЕ НЕПРЕРЫВНЫХ ЦЕПОЧЕК (ПО ГОДАМ) ===")
    top_chains.show(truncate=False)

    top_list = top_chains.collect()
    if len(top_list) >= 3:
        third = top_list[2]
        print(f"\n🎯 {n}-я по длине цепочка:")
        print(f"   Пользователь: {third['userid']}")
        print(f"   Длина цепочки: {third['chain_length']}")
    else:
        print(f"\nНайдено цепочек: {len(top_list)}")

    spark.stop()


if __name__ == "__main__":
    main()

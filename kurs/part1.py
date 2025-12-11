# Найти пользователя, имеющего наибольшую скорость написания сообщений.
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit part1.py <tweets_csv>")
        sys.exit(1)
    
    tweetspath = sys.argv[1]
    spark = SparkSession.builder \
        .appName("UserSpeed") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
    
    # Загружаем данные
    tweetsraw = spark.read.csv(tweetspath, header=True, inferSchema=True)
    required_cols = ["userid", "tweet_time"]
    missing = set(required_cols) - set(tweetsraw.columns)
    if missing:
        print(f"Available columns: {tweetsraw.columns}")
        raise ValueError(f"Missing columns in tweets CSV: {missing}")
    
    print(f"Total rows: {tweetsraw.count()}")
    
    # Подготавливаем данные
    tweets = tweetsraw.select(
        col("userid").cast("string").alias("userid"),
        col("tweet_time").cast("string").alias("tweet_time")
    ).filter(col("tweet_time").isNotNull() & col("userid").isNotNull())
    
    print(f"Valid rows: {tweets.count()}")
    
    print("=== RDD SOLUTION ===")
    # RDD решение (твиты / уникальные годы)
    def extract_year(t):
        import re
        match = re.search(r'(\d{4})', t)
        return int(match.group(1)) if match else None
    
    tweets_rdd = tweets.rdd.map(lambda row: (row.userid, row.tweet_time))
    user_stats_rdd = (tweets_rdd
        .filter(lambda x: x[1] is not None)
        .map(lambda x: (x[0], extract_year(x[1])))
        .filter(lambda x: x[1] is not None)
        .groupByKey()
        .mapValues(lambda years: (
            len([y for y in years if y is not None]),  # tweet_count
            len(set([y for y in years if y is not None]))  # unique_years
        ))
        .filter(lambda x: x[1][0] > 1)
        .mapValues(lambda x: x[0] * 1.0 / max(x[1], 1.0))  # speed_per_year
    )
    
    rdd_result = user_stats_rdd.take(1)
    if rdd_result:
        fastest_user_rdd = user_stats_rdd.reduce(lambda a, b: a if a[1] > b[1] else b)
        print("RDD RESULT:")
        print(f"User: {fastest_user_rdd[0]}")
        print(f"Speed: {fastest_user_rdd[1]:.4f} tweets/year")
        rdd_user = fastest_user_rdd[0]
    else:
        print("RDD RESULT: No users with multiple tweets found")
        rdd_user = None
    
    print("\n=== SPARKSQL SOLUTION ===")
    # DataFrame + SparkSQL решение (ТА ЖЕ МЕТРИКА)
    tweets_year = tweets.withColumn("year", 
        regexp_extract(col("tweet_time"), r"(\d{4})", 1).cast("int"))
    tweets_year_clean = tweets_year.filter(col("year").isNotNull())
    
    tweets_year_clean.createOrReplaceTempView("tweets_year")
    
    query = """
    SELECT 
        userid,
        tweet_count,
        unique_years,
        tweet_count * 1.0 / GREATEST(unique_years, 1.0) AS speed_per_year
    FROM (
        SELECT 
            userid,
            COUNT(*) AS tweet_count,
            COUNT(DISTINCT year) AS unique_years
        FROM tweets_year
        WHERE year IS NOT NULL
        GROUP BY userid
        HAVING COUNT(*) > 1
    )
    ORDER BY speed_per_year DESC
    LIMIT 1
    """
    
    result_df = spark.sql(query)
    print("SQL RESULT:")
    result_df.show(truncate=False)
    
    sql_result = result_df.collect()
    sql_user = sql_result[0]['userid'] if sql_result else None
    
    print(f"\n=== VERIFICATION ===")
    if rdd_user == sql_user:
        print(" RDD и SparkSQL дают одинаковый результат!")
        print(f"Победитель: {rdd_user}")
    else:
        print(" RDD и SparkSQL результаты различаются!")
        print(f"RDD: {rdd_user}, SQL: {sql_user}")
    
    spark.stop()

if __name__ == "__main__":
    main()

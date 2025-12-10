import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count


def main():
    if len(sys.argv) != 4:
        print("Usage: python part1.py <tweets_csv> <n_percent> <m_replies>")
        sys.exit(1)

    tweets_path = sys.argv[1]
    n = float(sys.argv[2])
    m = int(sys.argv[3])

    spark = SparkSession.builder.appName("HW_Part1_Var3").getOrCreate()

    tweets_raw = spark.read.csv(
        tweets_path,
        header=True,
        inferSchema=False,
    )

    required_cols = {"userid", "reply_count"}
    missing = required_cols - set(tweets_raw.columns)
    if missing:
        raise ValueError("Missing columns in tweets CSV: " + ", ".join(missing))

    tweets = tweets_raw.select(
        col("userid").cast("string").alias("userid"),
        col("reply_count").alias("reply_count_str"),
    )

    from pyspark.sql.functions import when as sql_when, lit

    tweets = tweets.withColumn(
        "reply_count",
        sql_when(
            (col("reply_count_str").isNull()) | (col("reply_count_str") == ""),
            lit(None).try_cast("long"),
        ).otherwise(col("reply_count_str").try_cast("long")),
    ).select("userid", "reply_count")

    # 1. Решение через RDD

    def mark_good(row):
        rc = row["reply_count"]
        good = 1 if (rc is not None and rc >= m) else 0
        return (row["userid"], good, 1)

    rdd = tweets.rdd.map(mark_good)

    agg_rdd = rdd.map(lambda x: (x[0], (x[1], x[2]))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    filtered_rdd = agg_rdd.filter(
        lambda kv: kv[1][1] > 0 and (kv[1][0] * 100.0 / kv[1][1]) >= n
    )

    best_user_rdd = (
        filtered_rdd.map(lambda kv: (kv[0], kv[1][0] / float(kv[1][1]), kv[1][1]))
        .sortBy(lambda x: (-x[1], -x[2]))
        .take(1)
    )

    if best_user_rdd:
        rdd_userid, rdd_share, rdd_total = best_user_rdd[0]
    else:
        rdd_userid, rdd_share, rdd_total = None, None, None

    print("=== RDD RESULT ===")
    print("userid:", rdd_userid)
    print("good_share:", rdd_share)
    print("total_tweets:", rdd_total)

    # 2. Решение через DataFrame + SparkSQL

    stats_df = tweets.groupBy("userid").agg(
        count("*").alias("total_cnt"),
        spark_sum(sql_when(col("reply_count") >= m, 1).otherwise(0)).alias("good_cnt"),
    )

    stats_df.createOrReplaceTempView("user_stats")

    query = f"""
    SELECT
        userid,
        good_cnt,
        total_cnt,
        (good_cnt * 1.0 / total_cnt) AS good_share
    FROM user_stats
    WHERE total_cnt > 0
      AND (good_cnt * 100.0 / total_cnt) >= {n}
    ORDER BY good_share DESC, total_cnt DESC
    LIMIT 1
    """

    best_user_df = spark.sql(query)

    print("=== SQL RESULT ===")
    best_user_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

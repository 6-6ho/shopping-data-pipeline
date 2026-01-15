"""
Spark Batch Job: Review Analysis

Analyzes review text data to extract keywords, sentiment, and insights.
Demonstrates NLP processing on unstructured text data.
"""

import sys
import os
from datetime import datetime, timedelta

# Add common module to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, lower, regexp_extract,
    explode, split, trim, length, lit, array, udf, countDistinct
)
from pyspark.sql.types import ArrayType, StringType, FloatType

from common.spark_utils import (
    create_spark_session,
    ensure_namespace,
    run_idempotent_overwrite
)


# Sentiment keywords
POSITIVE_KEYWORDS = [
    '좋아요', '좋아', '최고', '만족', '추천', '재구매', '빨라요', '빠르', '빠른',
    '괜찮', '예뻐', '예쁘', '훌륭', '대박', '감사', '친절', '꼼꼼', '완벽'
]

NEGATIVE_KEYWORDS = [
    '별로', '실망', '늦', '아쉬', '환불', '싸구려', '안좋', '나빠', '나쁘',
    '불량', '찢어', '고장', '불만', '최악', '짜증', '화나', '후회'
]

ASPECT_KEYWORDS = {
    'delivery': ['배송', '도착', '배달', '운송'],
    'quality': ['품질', '퀄리티', '재질', '소재', '마감'],
    'price': ['가격', '가성비', '저렴', '비싸', '싸', '비쌈'],
    'packaging': ['포장', '박스', '패키지', '패키징'],
    'size': ['사이즈', '크기', '크', '작', '맞', '핏'],
    'design': ['디자인', '색상', '색깔', '모양', '스타일']
}


def create_analysis_tables(spark):
    """Create tables for review analysis results."""

    # Daily review summary
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.review_daily_summary (
            report_date DATE,
            total_reviews LONG,
            avg_rating DOUBLE,
            rating_1 LONG,
            rating_2 LONG,
            rating_3 LONG,
            rating_4 LONG,
            rating_5 LONG,
            positive_count LONG,
            negative_count LONG,
            with_images_count LONG,
            avg_text_length DOUBLE
        )
        USING iceberg
        PARTITIONED BY (months(report_date))
    """)

    # Keyword frequency
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.review_keywords (
            report_date DATE,
            keyword STRING,
            keyword_type STRING,
            frequency LONG,
            avg_rating_with_keyword DOUBLE
        )
        USING iceberg
        PARTITIONED BY (report_date)
    """)

    # Product review summary
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.product_review_summary (
            report_date DATE,
            product_id STRING,
            review_count LONG,
            avg_rating DOUBLE,
            positive_ratio DOUBLE,
            negative_ratio DOUBLE,
            top_positive_keywords STRING,
            top_negative_keywords STRING
        )
        USING iceberg
        PARTITIONED BY (report_date)
    """)

    # Aspect-based sentiment
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.review_aspects (
            report_date DATE,
            aspect STRING,
            mention_count LONG,
            positive_mentions LONG,
            negative_mentions LONG,
            sentiment_score DOUBLE
        )
        USING iceberg
        PARTITIONED BY (report_date)
    """)

    print("Review analysis tables created")


def extract_keywords_udf():
    """UDF to extract keywords from text."""

    def extract_keywords(text):
        if not text:
            return []

        text_lower = text.lower()
        found_keywords = []

        for keyword in POSITIVE_KEYWORDS + NEGATIVE_KEYWORDS:
            if keyword in text_lower:
                found_keywords.append(keyword)

        return found_keywords

    return udf(extract_keywords, ArrayType(StringType()))


def calculate_sentiment_udf():
    """UDF to calculate simple sentiment score."""

    def calculate_sentiment(text):
        if not text:
            return 0.0

        text_lower = text.lower()
        positive_count = sum(1 for kw in POSITIVE_KEYWORDS if kw in text_lower)
        negative_count = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text_lower)

        total = positive_count + negative_count
        if total == 0:
            return 0.0

        return (positive_count - negative_count) / total

    return udf(calculate_sentiment, FloatType())


def run_daily_summary(spark, target_date: str):
    """Generate daily review summary."""

    print(f"Generating review summary for {target_date}")

    reviews_df = spark.read.table("iceberg.shopping.reviews") \
        .filter(col("created_date") == target_date)

    if reviews_df.count() == 0:
        print(f"No reviews for {target_date}")
        return

    # Add sentiment column
    sentiment_udf = calculate_sentiment_udf()
    reviews_with_sentiment = reviews_df \
        .withColumn("sentiment_score", sentiment_udf(col("review_text"))) \
        .withColumn("is_positive", col("sentiment_score") > 0) \
        .withColumn("is_negative", col("sentiment_score") < 0)

    # Calculate summary
    summary = reviews_with_sentiment.agg(
        count("*").alias("total_reviews"),
        avg("rating").alias("avg_rating"),
        spark_sum((col("rating") == 1).cast("long")).alias("rating_1"),
        spark_sum((col("rating") == 2).cast("long")).alias("rating_2"),
        spark_sum((col("rating") == 3).cast("long")).alias("rating_3"),
        spark_sum((col("rating") == 4).cast("long")).alias("rating_4"),
        spark_sum((col("rating") == 5).cast("long")).alias("rating_5"),
        spark_sum(col("is_positive").cast("long")).alias("positive_count"),
        spark_sum(col("is_negative").cast("long")).alias("negative_count"),
        spark_sum(col("has_images").cast("long")).alias("with_images_count"),
        avg("text_length").alias("avg_text_length")
    ).withColumn("report_date", lit(target_date).cast("date"))

    run_idempotent_overwrite(
        spark=spark,
        df=summary,
        table_name="iceberg.shopping.review_daily_summary",
        partition_col="report_date",
        partition_value=target_date
    )
    print("Daily review summary saved (Overwrite mode)")


def run_keyword_analysis(spark, target_date: str):
    """Analyze keyword frequency in reviews."""

    print(f"Analyzing keywords for {target_date}")

    reviews_df = spark.read.table("iceberg.shopping.reviews") \
        .filter(col("created_date") == target_date)

    if reviews_df.count() == 0:
        return

    # Extract keywords
    keywords_udf = extract_keywords_udf()
    reviews_with_keywords = reviews_df \
        .withColumn("keywords", keywords_udf(col("review_text")))

    # Explode and count keywords
    keyword_counts = reviews_with_keywords \
        .select("rating", explode("keywords").alias("keyword")) \
        .groupBy("keyword") \
        .agg(
            count("*").alias("frequency"),
            avg("rating").alias("avg_rating_with_keyword")
        ) \
        .withColumn("report_date", lit(target_date).cast("date")) \
        .withColumn("keyword_type",
            when(col("keyword").isin(POSITIVE_KEYWORDS), "positive")
            .when(col("keyword").isin(NEGATIVE_KEYWORDS), "negative")
            .otherwise("neutral")
        )

    run_idempotent_overwrite(
        spark=spark,
        df=keyword_counts,
        table_name="iceberg.shopping.review_keywords",
        partition_col="report_date",
        partition_value=target_date
    )
    print(f"Keyword analysis saved: {keyword_counts.count()} keywords")


def run_aspect_analysis(spark, target_date: str):
    """Analyze sentiment by aspect (delivery, quality, price, etc.)."""

    print(f"Analyzing aspects for {target_date}")

    reviews_df = spark.read.table("iceberg.shopping.reviews") \
        .filter(col("created_date") == target_date)

    if reviews_df.count() == 0:
        return

    sentiment_udf = calculate_sentiment_udf()

    results = []
    for aspect, keywords in ASPECT_KEYWORDS.items():
        # Filter reviews mentioning this aspect
        pattern = '|'.join(keywords)
        aspect_reviews = reviews_df \
            .filter(lower(col("review_text")).rlike(pattern)) \
            .withColumn("sentiment", sentiment_udf(col("review_text")))

        if aspect_reviews.count() > 0:
            stats = aspect_reviews.agg(
                count("*").alias("mention_count"),
                spark_sum((col("sentiment") > 0).cast("long")).alias("positive_mentions"),
                spark_sum((col("sentiment") < 0).cast("long")).alias("negative_mentions"),
                avg("sentiment").alias("sentiment_score")
            ).collect()[0]

            results.append({
                'report_date': datetime.strptime(target_date, "%Y-%m-%d").date(),
                'aspect': aspect,
                'mention_count': stats.mention_count,
                'positive_mentions': stats.positive_mentions or 0,
                'negative_mentions': stats.negative_mentions or 0,
                'sentiment_score': float(stats.sentiment_score or 0)
            })

    if results:
        from pyspark.sql import Row
        aspects_df = spark.createDataFrame([Row(**r) for r in results])
        
        run_idempotent_overwrite(
            spark=spark,
            df=aspects_df,
            table_name="iceberg.shopping.review_aspects",
            partition_col="report_date",
            partition_value=target_date
        )
        print(f"Aspect analysis saved: {len(results)} aspects")


def run_product_summary(spark, target_date: str):
    """Generate per-product review summary."""

    print(f"Generating product review summary for {target_date}")

    reviews_df = spark.read.table("iceberg.shopping.reviews") \
        .filter(col("created_date") == target_date)

    if reviews_df.count() == 0:
        return

    sentiment_udf = calculate_sentiment_udf()

    product_summary = reviews_df \
        .withColumn("sentiment", sentiment_udf(col("review_text"))) \
        .groupBy("product_id") \
        .agg(
            count("*").alias("review_count"),
            avg("rating").alias("avg_rating"),
            avg((col("sentiment") > 0).cast("double")).alias("positive_ratio"),
            avg((col("sentiment") < 0).cast("double")).alias("negative_ratio")
        ) \
        .withColumn("report_date", lit(target_date).cast("date")) \
        .withColumn("top_positive_keywords", lit("")) \
        .withColumn("top_negative_keywords", lit(""))

    run_idempotent_overwrite(
        spark=spark,
        df=product_summary,
        table_name="iceberg.shopping.product_review_summary",
        partition_col="report_date",
        partition_value=target_date
    )
    print(f"Product review summary saved: {product_summary.count()} products")


def main():
    """Main entry point."""

    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Starting review analysis for {target_date}")

    spark = create_spark_session("ReviewAnalysis")
    ensure_namespace(spark, "iceberg.shopping")
    create_analysis_tables(spark)

    try:
        run_daily_summary(spark, target_date)
        run_keyword_analysis(spark, target_date)
        run_aspect_analysis(spark, target_date)
        run_product_summary(spark, target_date)
        print(f"Review analysis completed for {target_date}")
    except Exception as e:
        print(f"Error in review analysis: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parents[1]

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BeFly - Silver to Gold")
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def write_gold(df, output_name: str):
    output_path = GOLD_DIR / output_name

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )

    print(f"Gold criada: {output_path}")


def main():
    spark = create_spark_session()

    print("Lendo dados da camada Silver...")

    silver_df = spark.read.parquet(str(SILVER_DIR / "bookings_enriched"))

    print("Criando Gold: revenue_by_hotel_month...")

    revenue_by_hotel_month = (
        silver_df
        .groupBy(
            "hotel",
            "arrival_date_year",
            "arrival_date_month_num"
        )
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum(
                F.when(F.col("is_canceled") == 0, 1).otherwise(0)
            ).alias("effective_bookings"),
            F.sum(
                F.when(F.col("is_canceled") == 1, 1).otherwise(0)
            ).alias("cancelled_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue_eur"),
            F.round(
                F.avg(F.when(F.col("is_canceled") == 0, F.col("adr"))),
                2
            ).alias("avg_adr_eur"),
            F.sum(
                F.when(F.col("is_canceled") == 0, F.col("total_nights")).otherwise(0)
            ).alias("total_nights_sold"),
            F.round(
                (
                    F.sum(F.when(F.col("is_canceled") == 1, 1).otherwise(0))
                    / F.count("*")
                ) * 100,
                2
            ).alias("cancellation_rate_pct")
        )
        .orderBy("arrival_date_year", "arrival_date_month_num", "hotel")
    )

    write_gold(revenue_by_hotel_month, "revenue_by_hotel_month")

    print("Criando Gold: cancellation_by_segment...")

    cancellation_by_segment = (
        silver_df
        .groupBy(
            "market_segment",
            "customer_type",
            "distribution_channel"
        )
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum(
                F.when(F.col("is_canceled") == 1, 1).otherwise(0)
            ).alias("cancelled_bookings"),
            F.round(
                (
                    F.sum(F.when(F.col("is_canceled") == 1, 1).otherwise(0))
                    / F.count("*")
                ) * 100,
                2
            ).alias("cancellation_rate_pct"),
            F.round(F.avg("lead_time"), 2).alias("avg_lead_time"),
            F.round(
                F.avg("total_of_special_requests"),
                2
            ).alias("avg_total_special_requests")
        )
        .orderBy(F.desc("cancellation_rate_pct"))
    )

    write_gold(cancellation_by_segment, "cancellation_by_segment")

    print("Criando Gold: top_countries_by_revenue...")

    top_countries_by_revenue = (
        silver_df
        .filter(F.col("is_canceled") == 0)
        .groupBy(
            "country",
            "country_name",
            "continent"
        )
        .agg(
            F.count("*").alias("effective_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue_eur"),
            F.round(F.avg("revenue"), 2).alias("avg_ticket_eur"),
            F.round(F.avg("lead_time"), 2).alias("avg_lead_time")
        )
        .orderBy(F.desc("total_revenue_eur"))
        .limit(20)
    )

    write_gold(top_countries_by_revenue, "top_countries_by_revenue")

    print("Criando Gold: guest_stay_profile...")

    guest_stay_profile = (
        silver_df
        .groupBy(
            "hotel",
            "customer_type"
        )
        .agg(
            F.count("*").alias("total_bookings"),
            F.round(F.avg("total_nights"), 2).alias("avg_total_nights"),
            F.round(F.avg("total_guests"), 2).alias("avg_total_guests"),
            F.round(F.avg("is_long_stay") * 100, 2).alias("pct_long_stay"),
            F.round(F.avg("is_family") * 100, 2).alias("pct_family")
        )
        .orderBy("hotel", "customer_type")
    )

    write_gold(guest_stay_profile, "guest_stay_profile")

    print("Camada Gold criada com sucesso.")

    spark.stop()


if __name__ == "__main__":
    main()

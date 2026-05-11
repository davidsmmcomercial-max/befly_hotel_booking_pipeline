from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parents[1]

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BeFly - Bronze to Silver")
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    print("Lendo dados da camada Bronze...")

    bronze_bookings = spark.read.parquet(str(BRONZE_DIR / "bookings"))
    bronze_countries = spark.read.parquet(str(BRONZE_DIR / "countries"))
    bronze_hotels = spark.read.parquet(str(BRONZE_DIR / "hotels"))

    month_map = F.create_map(
        [F.lit(x) for x in [
            "January", 1,
            "February", 2,
            "March", 3,
            "April", 4,
            "May", 5,
            "June", 6,
            "July", 7,
            "August", 8,
            "September", 9,
            "October", 10,
            "November", 11,
            "December", 12,
        ]]
    )

    int_cols = [
        "is_canceled",
        "lead_time",
        "arrival_date_year",
        "arrival_date_week_number",
        "arrival_date_day_of_month",
        "stays_in_weekend_nights",
        "stays_in_week_nights",
        "adults",
        "children",
        "babies",
        "is_repeated_guest",
        "previous_cancellations",
        "previous_bookings_not_canceled",
        "booking_changes",
        "days_in_waiting_list",
        "required_car_parking_spaces",
        "total_of_special_requests",
    ]

    silver_df = bronze_bookings

    print("Convertendo tipos...")

    for col_name in int_cols:
        silver_df = silver_df.withColumn(
            col_name,
            F.expr(f"try_cast({col_name} as int)")
        )

    silver_df = (
        silver_df
        .withColumn("adr", F.expr("try_cast(adr as double)"))
        .withColumn("reservation_status_date", F.to_date("reservation_status_date"))
        .withColumn("children", F.coalesce(F.col("children"), F.lit(0)))
        .withColumn("country", F.coalesce(F.col("country"), F.lit("UNK")))
        .withColumn("arrival_date_month_num", month_map[F.col("arrival_date_month")])
        .withColumn(
            "arrival_date",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("arrival_date_year"),
                    F.lpad(F.col("arrival_date_month_num"), 2, "0"),
                    F.lpad(F.col("arrival_date_day_of_month"), 2, "0"),
                )
            )
        )
        .withColumn(
            "total_nights",
            F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights")
        )
        .withColumn(
            "total_guests",
            F.col("adults") + F.col("children") + F.col("babies")
        )
        .withColumn(
            "is_family",
            F.when((F.col("children") > 0) | (F.col("babies") > 0), 1).otherwise(0)
        )
        .withColumn(
            "is_long_stay",
            F.when(F.col("total_nights") > 7, 1).otherwise(0)
        )
        .withColumn(
            "revenue",
            F.when(
                F.col("is_canceled") == 0,
                F.col("adr") * F.col("total_nights")
            ).otherwise(0)
        )
        .withColumn(
            "booking_status",
            F.when(F.col("reservation_status") == "Canceled", "Canceled")
            .when(F.col("reservation_status") == "No-Show", "NoShow")
            .when(F.col("reservation_status") == "Check-Out", "CheckedOut")
            .otherwise("Unknown")
        )
    )

    print("Aplicando filtros de qualidade...")

    before_count = silver_df.count()

    silver_df = silver_df.filter(F.col("total_guests") > 0)
    after_guest_filter_count = silver_df.count()

    silver_df = silver_df.filter(F.col("adr") >= 0)
    after_adr_filter_count = silver_df.count()

    print(f"Registros antes dos filtros: {before_count}")
    print(f"Removidos sem hóspedes: {before_count - after_guest_filter_count}")
    print(f"Removidos com ADR negativo: {after_guest_filter_count - after_adr_filter_count}")
    print(f"Registros finais antes do enriquecimento: {after_adr_filter_count}")

    print("Enriquecendo com tabelas de referência...")

    silver_enriched_df = (
        silver_df
        .join(
            bronze_countries,
            silver_df["country"] == bronze_countries["country_code"],
            "left"
        )
        .join(
            bronze_hotels,
            silver_df["hotel"] == bronze_hotels["hotel_name"],
            "left"
        )
        .withColumnRenamed("city", "hotel_city")
        .withColumnRenamed("star_rating", "hotel_star_rating")
        .withColumnRenamed("opened_year", "hotel_opened_year")
        .drop("country_code", "hotel_name")
    )

    silver_before_join_count = silver_df.count()
    silver_after_join_count = silver_enriched_df.count()

    print(f"Registros antes do enriquecimento: {silver_before_join_count}")
    print(f"Registros após enriquecimento: {silver_after_join_count}")

    if silver_before_join_count == silver_after_join_count:
        print("Validação OK: joins não duplicaram registros.")
    else:
        print("Atenção: joins alteraram a quantidade de registros.")

    print("Gravando camada Silver...")

    (
        silver_enriched_df.write
        .mode("overwrite")
        .partitionBy("arrival_date_year", "arrival_date_month_num")
        .parquet(str(SILVER_DIR / "bookings_enriched"))
    )

    print("Camada Silver criada com sucesso.")
    print(f"Path: {SILVER_DIR / 'bookings_enriched'}")

    spark.stop()


if __name__ == "__main__":
    main()

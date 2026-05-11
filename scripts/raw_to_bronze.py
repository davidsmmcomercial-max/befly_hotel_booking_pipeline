from pathlib import Path

from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BeFly - Raw to Bronze")
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, file_name: str):
    return (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("inferSchema", True)
        .option("nullValue", "NULL")
        .option("nanValue", "NA")
        .csv(str(RAW_DIR / file_name))
    )


def write_parquet(df, output_path: Path, partition_cols=None):
    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(str(output_path))


def main():
    spark = create_spark_session()

    print("Lendo arquivos da camada RAW...")

    bookings_df = read_csv(spark, "hotel_bookings.csv")
    countries_df = read_csv(spark, "country_metadata.csv")
    hotels_df = read_csv(spark, "hotel_metadata.csv")

    print(f"Bookings: {bookings_df.count()} registros")
    print(f"Countries: {countries_df.count()} registros")
    print(f"Hotels: {hotels_df.count()} registros")

    print("Gravando camada BRONZE...")

    write_parquet(
        bookings_df,
        BRONZE_DIR / "bookings",
        partition_cols=["arrival_date_year"]
    )

    write_parquet(countries_df, BRONZE_DIR / "countries")
    write_parquet(hotels_df, BRONZE_DIR / "hotels")

    print("Camada Bronze criada com sucesso.")
    print(f"Path: {BRONZE_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()

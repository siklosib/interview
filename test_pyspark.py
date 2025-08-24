from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("local-spark-test") \
        .master("local[*]") \
        .getOrCreate()

    # Create a small DataFrame
    data = [("Alice", 29), ("Bob", 31), ("Charlie", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Simple transformation
    df_filtered = df.filter(df.age > 27)

    # Show results
    print("=== Spark Local Test ===")
    df_filtered.show()

    spark.stop()

if __name__ == "__main__":
    main()

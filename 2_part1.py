from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    current_timestamp,
    regexp_replace,
    from_json,
)

from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from configs import kafka_config

# Визначення нових топіків
my_name = "lina"
athlete_event_results = f"{my_name}_athlete_event_results"
aggregated_athlete_stats = f"{my_name}_aggregated_athlete_stats"

# Створення Spark сесії
spark = (
    SparkSession.builder.appName("FP1")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "mysql:mysql-connector-java:8.0.32,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .getOrCreate()
)

# Визначення схеми для JSON-даних
json_schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
    ]
)

# 1. Зчитування даних з MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/neo_data"
db_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}

athlete_bio_df = spark.read.jdbc(
    url=jdbc_url,
    table="olympic_dataset.athlete_bio",
    properties=db_properties,
    column="athlete_id",  # Колонка для партиціонування
    lowerBound=1,  # Мінімальне значення athlete_id
    upperBound=1000000,  # Максимальне значення athlete_id
    numPartitions=10,  # Кількість партицій
)

# 2. Фільтрація даних за зростом і вагою
athlete_bio_filtered_df = (
    athlete_bio_df.withColumn("height", col("height").cast(DoubleType()))
    .withColumn("weight", col("weight").cast(DoubleType()))
    .filter((col("height").isNotNull()) & (col("weight").isNotNull()))
)

print("Athlete H/W Data:")
athlete_bio_filtered_df.show()

# 3. Зчитування даних з MySQL і запис у Kafka
athlete_event_results_df = spark.read.jdbc(
    url=jdbc_url,
    table="olympic_dataset.athlete_event_results",
    properties=db_properties,
    column="result_id",  # Колонка для партиціонування
    lowerBound=1,  # Мінімальне значення result_id
    upperBound=100000,  # Максимальне значення result_id
    numPartitions=10,  # Кількість партицій
)

print("Events Results:")
athlete_event_results_df.show()

# запис даних у Kafka
athlete_event_results_df.selectExpr(
    "CAST(result_id AS STRING) AS key",
    "to_json(struct(athlete_id, sport, medal)) AS value",
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config",
    f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';",
).option(
    "topic", athlete_event_results
).save()


# Читання даних з Kafka у стрімінговий DataFrame
data_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';",
    )
    .option("subscribe", "athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), json_schema).alias("data"))
    .select("data.athlete_id", "data.sport", "data.medal")
)

# Виведення отриманих даних на екран
# data_from_kafka.writeStream.trigger(availableNow=True).outputMode("append").format(
#     "console"
# ).option("truncate", "false").start().awaitTermination()

# # Об’єднання стрімінгових даних
combined_df = data_from_kafka.join(
    athlete_bio_filtered_df, on="athlete_id", how="inner"
)

combined_df.writeStream.trigger(availableNow=True).outputMode("append").format(
    "console"
).option("truncate", "false").start().awaitTermination()

# # Обчислення середнього зросту і ваги атлетів
aggregated_df = combined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

print("PRINT aggregated_df:")
aggregated_df.writeStream.trigger(availableNow=True).outputMode("complete").format(
    "console"
).option("truncate", "false").start().awaitTermination()


def process_and_write(batch_df, batch_id):
    # Перший запис у Kafka
    batch_df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config",
        f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';",
    ).option(
        "topic", aggregated_athlete_stats
    ).save()

    # Другий запис у базу даних
    batch_df.write.jdbc(
        url=jdbc_url,
        table=aggregated_athlete_stats,
        mode="append",
        properties=db_properties,
    )


aggregated_df.writeStream.foreachBatch(process_and_write).outputMode("complete").option(
    "checkpointLocation", "/tmp/checkpoints-FP1"
).start()


spark.streams.awaitAnyTermination()

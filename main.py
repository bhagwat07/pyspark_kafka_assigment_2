from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,when,window,udf
from pyspark.sql.types import *


spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("KafkaEventProcessor")
        .config('spark.jars',"C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config('spark.jars',"C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\mysql-connector-java-8.0.12.jar")
        .config("spark.executor.extraClassPath","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config("spark.executor.extraLibrary","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config("spark.driver.extraClassPath","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .getOrCreate()
    )


#To ignore the Spark's correctness checks on streaming operations.
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")


#creating a function to pass foreachBatch function to print ouput on console
def foreach_batch_function(df, epoch_id):
    # This function is called for each batch of data
    df.show(truncate=False, vertical=False)


kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "event"
}


kafka_messages = (spark
                    .readStream
                    .format("kafka")
                    .options(**kafka_params)
                    .load()
                  )


# sch = StructType([
#     StructField("date_time", StringType(), True),
#     StructField("advertiserId", StringType(), True),
#     StructField("adslotId", StringType(), True),
#     StructField("uniqId", StringType(), True),
#     StructField("product_name", StringType(), True),
#     StructField("show", StringType(), True),
#     StructField("amount", DoubleType(), True)
# ])


#creating a calculate_amount function to calculate final_amount.
def calculate_amount(show, amount, amount_charge):
    if show == "yes":
        return amount
    elif amount_charge is not None:
        return amount_charge
    else:
        return 0.0


# registering the UDF.
calculate_amount_udf = udf(calculate_amount, FloatType())

#process_message to handle message parsing and formatting,
def process_message(value):
    structure_df =  value.selectExpr("CAST(value AS STRING) as value").select(split(col('value'), '\\^').alias('message'))
    message_df = (structure_df.selectExpr("cast(message[0] as timestamp) as date_time", "message[1] as advertiserId", "message[2] as adslotId",
                                      "message[3] as uniqId", "message[4] as product_name", "message[5] as show", "cast(message[6] as double) as amount")
                  )


    return message_df

process_message_udf = udf(process_message)

process_df = kafka_messages.select(process_message_udf(kafka_messages['value']).alias("value"))

#Loading the Advertiser table from mysql.
advertiser_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/kafka_db") \
    .option("dbtable", "advertiser") \
    .option("user", "root") \
    .option("password", "bhagwat@mysql") \
    .load()
#Loading the Adslot_df table from mysql.
adslot_df = spark.read.format("jdbc") \
           .option("url", "jdbc:mysql://localhost:3306/kafka_db") \
           .option("dbtable", "adslot") \
           .option("user", "root") \
           .option("password", "bhagwat@mysql") \
           .load()

#Joining the both table using common_key.
joined_df = process_df.join(advertiser_df, ["advertiserId"], "left").join(adslot_df, ["adslotId"], "left")

#apply a calculate df function to calculate the required output.
combine_df = (joined_df.withColumn("amount",calculate_amount_udf(joined_df["show"],
                                                                            joined_df["amount"],
                                                                            joined_df["amount_charge"])))

# filter_data = joined_df.withColumn('show',when(col('show') == "yes", col('amount')).otherwise(when(col("amount_charge").isNotNull(), col("amount_charge")).otherwise(0.0)))


# Deduplicate based on uniqId and hour using watermark and window functions
nduplicate_df = combine_df.withColumn("hour", window("date_time", "1 hour")).dropDuplicates(["uniqId", "hour"])


#renaming the column
final_df = nduplicate_df.withColumnRenamed('advertiserId','advertiser_id')

#selecting the required column
final_df = final_df.select("date_time","amount","product_name","adslot_name","type_of_adslot","advertiser_name")

#for the display the ouput on console. use foreachBatch function.
query = final_df.writeStream.foreachBatch(foreach_batch_function).start()

query.awaitTermination()

# Write the output to HDFS
# query = output_stream.writeStream \
#     .format("json") \
#     .outputMode("append") \
#     .option("path", "hdfs://project/data/") \
#     .start()
#
# # Start the streaming query
# query.awaitTermination()

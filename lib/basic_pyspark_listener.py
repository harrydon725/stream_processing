from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sparksum  # <-- for aggregations
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import udf, lit

def parse_json_dataframe(json_df):
    def getKey(dictionary, key): 
        return dictionary.get(key)

    udfGetKey = udf(getKey)

    # Deserialize the JSON value from Kafka as a String
    # and convert it to a dict
    return json_df\
        .withColumn("value", from_json(
            col('value').cast("string"),
            MapType(StringType(), StringType())
        )) \
        .withColumn('order_id', udfGetKey(col('value'), lit('order_id'))) \
        .withColumn('amount', udfGetKey(col('value'), lit('amount'))) \
        .withColumn('customer_id', udfGetKey(col('value'), lit('customer_id'))) \
        .select('order_id', 'amount', 'customer_id')



kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",
    "startingOffsets": "latest", 
    "subscribe": "orders"        
}


spark = SparkSession.builder.appName("IntroToPySpark") \
    .config("spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1")\
    .getOrCreate()

df = spark.readStream.format("kafka").options(**kafka_options).load()

sum_df = parse_json_dataframe(df).groupBy('customer_id').agg(sparksum('amount'))

query = sum_df.writeStream.outputMode('update').format('console').start()


query.awaitTermination()

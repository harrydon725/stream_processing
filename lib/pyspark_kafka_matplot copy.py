from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import count
from IPython import display
import time
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import MapType,StringType,IntegerType
from pyspark.sql.functions import udf, lit
import pandas as pd

kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",
    "startingOffsets": "latest", # Start from the latest event when we consume from kafka
    "subscribe": "orders"        # Our topic name
}

spark = SparkSession.builder.appName("IntroToPySpark") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1') \
    .getOrCreate()

df = spark.readStream.format("kafka").options(**kafka_options).load()

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

parse_json_dataframe(df) \
    .writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("orderQuery") \
    .start()

# Basic display loop
while True:
    # Clear the previous plot
    display.clear_output(wait=True)

    dataframe = spark.sql("SELECT * FROM orderQuery").toPandas()
    dataframe['amount'] = pd.to_numeric(dataframe['amount'])
    aggregated = dataframe.groupby('customer_id')['amount'].sum().reset_index(name ='total_amount')

    # print(aggregated.to_string())

    # Do some plotting
    plt.figure(figsize=(10, 6))
    plt.bar(aggregated['customer_id'], aggregated['total_amount'])
    plt.title('Total Transaction Amounts by Customer')
    plt.xlabel('Customer ID')
    plt.ylabel('Total Amount')

    plt.show()

    time.sleep(3)  # Refresh every 3 seconds. Adjust this to your needs.

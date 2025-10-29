from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import count, from_json, col, sum as sparksum
from IPython import display
import time
from pyspark.sql.types import MapType,StringType,LongType
from pyspark.sql.functions import udf, lit
from sklearn.linear_model import LinearRegression
import plotext as plt_term


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
    .queryName("ordersQuery")\
    .start() \
    .awaitTermination()

dataframe = spark.sql("SELECT * FROM ordersQuery").toPandas()

# OKAY GARMIN: build aggregated streaming dataframe (count orders per customer)
agg_df = parse_json_dataframe(df) \
    .groupBy("customer_id") \
    .agg(F.count("*").alias("total_orders"))

# OKAY GARMIN: write aggregated results to an in-memory table so we can query it from this process
query = agg_df.writeStream \
    .format("memory") \
    .queryName("ordersAgg") \
    .outputMode("complete") \
    .start()

# OKAY GARMIN: poll the in-memory table and render an ASCII bar chart in the terminal
import time
while True:
    try:
        pdf = spark.sql("SELECT customer_id, total_orders FROM ordersAgg ORDER BY total_orders DESC").toPandas()
        if not pdf.empty:
            x = pdf['customer_id'].astype(str).tolist()
            y = pdf['total_orders'].tolist()
            plt_term.clear_figure()
            plt_term.bar(x, y)
            plt_term.title("Orders per customer")
            plt_term.show()
        else:
            print("No data yet in ordersAgg")
        time.sleep(3)
    except KeyboardInterrupt:
        print("Stopping loop")
        break

query.stop()




from os import getenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, udf
from pyspark.sql.types import TimestampType
import matplotlib.pypot as plt
import datetime
import binance




ApiKey = getenv("BINANCE_API_KEY")
ApiSecret = getenv("BINANCE_API_SECRET")

BinanceClient = binance.Client(ApiKey, ApiSecret)

# Define the symbol and the interval for the historical data
symbol = "BTCUSDT"
interval = BinanceClient.KLINE_INTERVAL_1WEEK 

# Get the historical data from Binance
klines = BinanceClient.futures_klines(symbol=symbol, interval=interval)

# Convert the klines to a list of tuples, where each tuple contains the timestamp and the close price
data = [(int(kline[0]), float(kline[4])) for kline in klines]

# Create a Spark session and a Spark context
spark = SparkSession.builder.appName("BinanceHistoricalData").getOrCreate()
sc = spark.sparkContext

# Create an RDD from the data
rdd = sc.parallelize(data)

# Convert the RDD to a DataFrame
df = spark.createDataFrame(rdd, ["timestamp", "close"])

# Convert bigint to datetime object (assuming timestamp is in milliseconds)
from_bigint_timestamp_udf = udf(lambda timestamp: datetime.datetime.fromtimestamp(timestamp / 1000.0), TimestampType())

# Convert timestamp column to UTC datetime format
df = df.withColumn("timestamp", from_bigint_timestamp_udf(col("timestamp")))

df.show()

pandas_df = df.select(col("timestamp"), col("close")).toPandas()

# Build the scatter plot
plt.scatter(pandas_df["timestamp"], pandas_df["close"])
plt.xlabel("datetime_utc")
plt.ylabel("price")
plt.title("Historical price change")
plt.show()
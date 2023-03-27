from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import getenv
import binance


ApiKey = getenv("BINANCE_API_KEY")
ApiSecret = getenv("BINANCE_API_SECRET")

BinanceClient = binance.Client(ApiKey, ApiSecret)

# Define the symbol and the interval for the historical data
symbol = "BTCUSDT"
interval = BinanceClient.KLINE_INTERVAL_1DAY

# Get the historical data from Binance
klines = BinanceClient.futures_klines(symbol=symbol, interval=interval)

# Create a Spark session and a Spark context
spark = SparkSession.builder.appName("BinanceHistoricalData").getOrCreate()
sc = spark.sparkContext

# Convert the klines to a list of tuples, where each tuple contains the timestamp and the close price
data = [(int(kline[0]), float(kline[4])) for kline in klines]

# Create an RDD from the data
rdd = sc.parallelize(data)

# Convert the RDD to a DataFrame
df = spark.createDataFrame(rdd, ['timestamp', 'close'])

# Save the DataFrame to HDFS in Parquet format
df.write.parquet('/binance/historical_data')
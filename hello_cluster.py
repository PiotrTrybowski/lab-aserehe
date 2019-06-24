from pyspark import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from operator import add
import pyspark.sql.functions as f


sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = spark.read.csv('s3://piotrtrybowski/200705hourly.csv', header=True)
#df[' Dry Bulb Temp'].show()
# counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).coalesce(1).saveAsTextFile('s3://piotrtrybowski/output.txt')
average_all = df.select(f.avg(' Dry Bulb Temp'),f.avg(' Wet Bulb Temp'),f.avg(' Wind Speed (kt)')).show()
average_daily = df.groupBy(' YearMonthDay')\
    .agg({' Dry Bulb Temp':"avg",' Wet Bulb Temp':"avg",' Wind Speed (kt)':"avg"})\
    .sort(' YearMonthDay')\
    .coalesce(1).write.option("header","true").csv('s3://piotrtrybowski/outputs/hourly_to_daily_avg.csv')


sc.stop()

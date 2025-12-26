#3. Count number of movie ratings per year. Hint: convert time column to TIMESTAMP using from_unixtime().

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .appName("Q_3")\
                  .getOrCreate()

filepath="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/movies/ratings.csv"
rating=spark.read\
            .option("header","true")\
            .option("infrStructur","true")\
            .csv(filepath)
rating.printSchema()

ratings=rating.withColumn("yr",year(from_unixtime("timestamp")))
ratings.show()
ratings_per_year=rating.groupBy(year(from_unixtime("timestamp")).alias("yr"))\
                      .count()
ratings_per_year.show()
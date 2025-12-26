# 1. Find max sal, min sal, avg sal, total sal per dept per job in emp.csv ﬁle.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .appName("Q_1")\
                  .getOrCreate()


filepath="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/emp.csv"
emp_shema=StructType([
    StructField("empno",IntegerType(),False),
    StructField("ename",StringType(),True),
    StructField("job",StringType(),True),
    StructField("mgr",IntegerType(),True),
    StructField("hire",DateType(),True),
    StructField("sal",DecimalType(9,2),True),
    StructField("comm",DecimalType(9,2),True),
    StructField("deptno",IntegerType(),True)
])
emps=spark.read\
          .option("header","false")\
          .schema(emp_shema)\
          .csv(filepath)
emps.printSchema()
emps.show()

result=emps.groupBy("deptno")\
           .agg(
               max("sal").alias("maxsal"),
               min("sal").alias("minsal"),
               avg("sal").alias("avgsal"),
               sum("sal").alias("totalsal")
           )\
           .orderBy("deptno")



result.printSchema()
result.show()

spark.stop()
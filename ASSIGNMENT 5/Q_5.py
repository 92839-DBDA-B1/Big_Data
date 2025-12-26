# 5. For each department, rank employees by their salary in descending order. Include employee name, department name, salary, and their rank within the
# department. Handle ties so employees with the same salary get the same rank, with the next rank(s) skipped.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder\
                  .appName("Q_5")\
                  .getOrCreate()
filepath="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/emp.csv"
emp_schema=StructType([
      StructField("empno",IntegerType(),False),
      StructField("ename",StringType(),True),
      StructField("job",StringType(),True),
      StructField("mgr",IntegerType(),True),
      StructField("hire",DateType(),True),
      StructField("sal",DoubleType(),True),
      StructField("comm",DecimalType(),True),
      StructField("deptno",IntegerType(),True)
])
emp=spark.read\
         .option("header","false")\
         .schema(emp_schema)\
         .csv(filepath)
emp.printSchema()
emp.show()

deptfilepath="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/dept.csv"
dept_schema=StructType([
    StructField("deptno",IntegerType(),False),
    StructField("dname",StringType(),True),
    StructField("location",StringType(),True)
])
dept=spark.read\
          .schema(dept_schema)\
          .csv(deptfilepath)
dept.printSchema()
dept.show()

wnd=Window.partitionBy("deptno").orderBy(desc("sal"))
result=emp\
        .withColumn("sr",row_number().over(wnd))\
        .withColumn("rnk",rank().over(wnd))\
        .withColumn("drnk",dense_rank().over(wnd))
result.show()

rank_with_dept=result\
                .join(dept,"deptno")\
                .select("ename","dname","deptno","sal","rnk")

rank_with_dept.show()
spark.stop()


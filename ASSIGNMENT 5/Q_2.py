#2. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal. Hint: use join()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .appName('Q_2')\
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


total_sal=emp.groupBy("deptno")\
             .agg(sum("sal").alias("total_sal"))
total_sal.show()
dept_sal=total_sal\
            .join(dept,"deptno")\
            .select("deptno","dname","total_sal")\
            .orderBy("deptno",ascending=False)
dept_sal.show()
spark.stop()



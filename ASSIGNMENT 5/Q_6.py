# 6. Salary Diﬀerence from Department Average For each employee, show how much their salary diﬀers from their department's average salary. Also show
# the department's maximum salary. - Output: ename, dname, sal, dept_avg_sal, sal_diﬀ_from_avg, dept_max_sal

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark=SparkSession.builder\
                  .appName("Q_6")\
                  .getOrCreate()
empfile="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/emp.csv"
emp_schema=StructType([
    StructField("empno",IntegerType(),False),
    StructField("ename",StringType(),True),
    StructField("job",StringType(),True),
    StructField("mgr",IntegerType(),True),
    StructField("hire",DateType(),True),
    StructField("sal",DoubleType(),True),
    StructField("comm",DoubleType(),True),
    StructField("deptno",IntegerType(),True),
])
emp=spark.read\
         .option("header","false")\
         .schema(emp_schema)\
         .csv(empfile)

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

result=emp.groupBy("deptno")\
          .agg(avg("sal").alias("avg_sal"),
               max("sal").alias("max_sal"))

result.show()

table=emp\
        .join(dept,"deptno")\
        .join(result,"deptno")\
        .withColumn("sal_diﬀ_from_avg",col("sal")-col("avg_sal"))\
        .select("ename","dname","sal","avg_sal","sal_diﬀ_from_avg","max_sal")
        
table.show()




spark.stop()

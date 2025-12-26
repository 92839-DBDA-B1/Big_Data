# 7. For each department, identify:
# ----The ﬁrst employee hired (earliest hire date)
# ----The last employee hired (most recent hire date)
# ----The employee hired immediately after each employee (by hire date)
# ---Output: deptno, dname, ename, hire, ﬁrst_hire_ﬂag, last_hire_ﬂag, next_hire_employee


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder\
                  .appName("Q_7")\
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


w=Window.partitionBy("deptno").orderBy("hire")
w_desc=Window.partitionBy("deptno").orderBy(desc("hire"))
ﬁrst_hire_ﬂag=emp.withColumn("ﬁrst_hire_ﬂag",when(row_number().over(w)==1,1).otherwise(0))\
                 .withColumn("last_hire_ﬂag",when(row_number().over(w_desc)==1,1).otherwise(0))\
                 .withColumn("next_hire_employee",lead("ename").over(w))

    
first_hire_flag.show()
table=ﬁrst_hire_ﬂag\
        .join(dept,"deptno")\
        .select("deptno","dname","ename","hire","ﬁrst_hire_ﬂag","last_hire_ﬂag","next_hire_employee")
table.show()

# result=emp\
#           .withColumn("next_hire_employee",lead("ename").over(w))

spark.stop()
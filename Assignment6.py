from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create session
spark = SparkSession.builder.appName("Assignment6").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# Read emp.csv into emps
filepath_emp = "/media/sunbeam/NewVolume/krishnankit/cdac/Notes/BigData/data/emp.csv"
emps = spark.read\
        .option("header", "false")\
        .schema(StructType([
            StructField("id", IntegerType(), False),
            StructField("ename", StringType()),
            StructField("job", StringType()),
            StructField("mgr", IntegerType()),
            StructField("hire", DateType()),
            StructField("sal", DecimalType(9, 2)),
            StructField("comm", DecimalType(9, 2)),
            StructField("deptno", IntegerType())
        ]))\
        .csv(filepath_emp)

# 1. Find max sal, min sal, avg sal, total sal per dept per job in emp.csv ﬁle.
result1 = emps.groupBy("deptno")\
            .agg(
                max("sal").alias("max_sal"),
                min("sal").alias("min_sal"),
                avg("sal").alias("avg_sal"),
                sum("sal").alias("sum_sal")
            )

# result1.show()

# 2. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal. Hint: use join()
# Read dept.csv into depts
filepath_dept = "/media/sunbeam/NewVolume/krishnankit/cdac/Notes/BigData/data/dept.csv"
depts = spark.read\
        .option("header", "false")\
        .schema(StructType([
            StructField("deptno", IntegerType(), False),
            StructField("dname", StringType()),
            StructField("location", StringType())
        ]))\
        .csv(filepath_dept)

result2 = emps.groupBy("deptno")\
            .sum("sal")\
            .join(depts, "deptno", "right")\
            .select("dname", ifnull("sum(sal)", lit(0.0)))\
            .withColumnRenamed("ifnull(sum(sal), 0.0)", "total_sal")\

# result2.show()

# Output
# Print dname and total sal.
# +----------+---------+                                                          
# |     dname|total_sal|
# +----------+---------+
# |ACCOUNTING|   8750.0|
# |  RESEARCH|  10875.0|
# |     SALES|   9400.0|
# |OPERATIONS|      0.0|
# +----------+---------+

# 3. Count number of movie ratings per year. Hint: convert time column to TIMESTAMP using from_unixtime().
# Read ragings.csv into ratings
filepath_ratings = "/media/sunbeam/NewVolume/krishnankit/cdac/Notes/BigData/data/movies/ratings.csv"
ratings = spark.read\
        .option("header", "true")\
        .schema(StructType([
            StructField("userId", IntegerType(), False),
            StructField("movieId", IntegerType(), False),
            StructField("rating", DecimalType(2, 1)),
            StructField("rtime", LongType())
        ]))\
        .csv(filepath_ratings)

result3 = ratings.select("rating", from_unixtime("rtime").alias("rtime"))\
            .groupBy(year("rtime").alias("year"))\
            .count()\
            .withColumnRenamed("count", "ratings_count")

# result3.show()

# Output
# +----+-------------+                                                            
# |year|ratings_count|
# +----+-------------+
# |2003|         4463|
# |2007|         1548|
# |2015|         6610|
# |2006|         7493|
# |2013|         1969|
# |1997|         3294|
# |2014|         2224|
# |2004|         4658|
# |1996|         6239|
# |1998|         1825|
# |2012|         3850|
# |2009|         3432|
# |2016|         6225|
# |1995|            3|
# |2001|         4658|
# |2005|         7161|
# |2000|        13869|
# |2010|         2520|
# |2011|         4449|
# |2008|         3676|
# +----+-------------+
# only showing top 20 rows


# 4. Load Fire Service Calls with pre-deﬁned schema. Repeat all 10 Hive assignments on that dataset. Do the assignments using Dataframe syntax. Use Linux
# command below to create sample dataset.
file_path_fire = "/media/sunbeam/NewVolume/krishnankit/cdac/my_notes/big_data/data/Fire_Department_Calls_for_Service/Fire_Department_Calls_for_Service.csv"
schema = ""

fire_data = spark.read\
                .option("header", "true")\
                .schema(StructType([
                    StructField("callNumber", StringType()),
                    StructField("unitId", StringType()),
                    StructField("incidentNumber", LongType()),
                    StructField("callType", StringType()),
                    StructField("callDate", StringType()),
                    StructField("watchDate", StringType()),
                    StructField("received", StringType()),
                    StructField("entry", StringType()),
                    StructField("dispatch", StringType()),
                    StructField("response", StringType()),
                    StructField("onScene", StringType()),
                    StructField("transport", StringType()),
                    StructField("hospital", StringType()),
                    StructField("finalDisposition", StringType()),
                    StructField("address", StringType()),
                    StructField("availableTime", StringType()),
                    StructField("city", StringType()),
                    StructField("zipcode", LongType()),
                    StructField("battalion", StringType()),
                    StructField("stationArea", IntegerType()),
                    StructField("box", IntegerType()),
                    StructField("originalPriority", IntegerType()),
                    StructField("priority", StringType()),
                    StructField("finalPriority", StringType()),
                    StructField("alsUnit", BooleanType()),
                    StructField("callTypeGroup", StringType()),
                    StructField("numberOfAlarms", IntegerType()),
                    StructField("unitType", StringType()),
                    StructField("unitSequence", IntegerType()),
                    StructField("firepreventionDist", IntegerType()),
                    StructField("supervisorDist", IntegerType()),
                    StructField("neighborhoods", StringType()),
                    StructField("rowId", StringType()),
                    StructField("caseLocation", StringType()),
                    StructField("dataAsOf", StringType()),
                    StructField("dataLoadedAt", StringType()),
                    StructField("analysisNeighborhoods", StringType()),
                ]))\
                .csv(file_path_fire)


# fire_data.printSchema()
# fire_data.show()
fire_df = fire_data\
            .withColumn("callDate", to_date("callDate", "dd/mm/yyyy"))\
            .withColumn("watchDate", to_date("watchDate", "dd/mm/yyyy"))\
            .withColumn("received", to_timestamp("received", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("entry", to_timestamp("entry", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("dispatch", to_timestamp("dispatch", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("response", to_timestamp("response", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("onScene", to_timestamp("onScene", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("transport", to_timestamp("transport", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("hospital", to_timestamp("hospital", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("availableTime", to_timestamp("availableTime", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("dataAsOf", to_timestamp("dataAsOf", "dd/mm/yyyy hh:mm:ss a"))\
            .withColumn("dataLoadedAt", to_timestamp("dataLoadedAt", "dd/mm/yyyy hh:mm:ss a"))\

# fire_df.printSchema()

# 4.1. How many distinct types of calls were made to the ﬁre department?
# call_types_count = fire_data.select("callType").distinct().count()
# Output: Number of distinct types of calls:  33  

# print("Number of distinct types of calls: ", call_types_count)

# 4.2 What are distinct types of calls made to the ﬁre department?
call_types = fire_data.select("callType").distinct()

# call_types.show(truncate=False)

# Output:
# +--------------------------------------------+                                  
# |callType                                    |
# +--------------------------------------------+
# |Elevator / Escalator Rescue                 |
# |Marine Fire                                 |
# |Aircraft Emergency                          |
# |Confined Space / Structure Collapse         |
# |Structure Fire / Smoke in Building          |
# |Administrative                              |
# |Alarms                                      |
# |Odor (Strange / Unknown)                    |
# |Citizen Assist / Service Call               |
# |HazMat                                      |
# |Watercraft in Distress                      |
# |Explosion                                   |
# |Oil Spill                                   |
# |Vehicle Fire                                |
# |Suspicious Package                          |
# |Extrication / Entrapped (Machinery, Vehicle)|
# |Other                                       |
# |Outside Fire                                |
# |Traffic Collision                           |
# |Assist Police                               |
# +--------------------------------------------+
# only showing top 20 rows

# 4.3. Find out all responses for delayed times greater than 5 mins?
delayed_responses = fire_df.withColumn("delay", unix_timestamp("response") - unix_timestamp("received") / 300).filter("delay > 5")\
                        .select("unitId", "incidentNumber", "received", "response", "delay")


delayed_responses.show()

# Output:
# +------+--------------+-------------------+-------------------+--------------------+
# |unitId|incidentNumber|           received|           response|               delay|
# +------+--------------+-------------------+-------------------+--------------------+
# |   B08|       8012119|2008-01-02 22:03:52|2008-01-02 22:07:10|1.1952941912266667E9|
# |   E33|       8004586|2008-01-01 11:16:19|2008-01-01 11:17:42|1.1951692407366667E9|
# |    93|       8044077|2008-01-05 11:20:59|2008-01-05 11:23:17|1.1955140228033333E9|
# |   E44|       8015324|2008-01-02 18:16:14|2008-01-02 18:18:18|1.1952805047533333E9|
# |    86|       8011046|2008-01-02 12:22:23|2008-01-02 12:24:35|1.1952593525233333E9|
# |    82|       8027441|2008-01-03 14:29:41|2008-01-03 14:31:08|1.1953530320633333E9|
# |   M22|       7103158|2007-01-12 21:32:59|2007-01-12 21:35:07|1.1647225144033334E9|
# |   T02|       8042538|2008-01-05 13:09:47|2008-01-05 13:13:27|1.1955206110433333E9|
# |   E03|       8029452|2008-01-04 13:05:34|2008-01-04 13:07:55|1.1954341678866668E9|
# |   B01|       8000704|2008-01-01 18:09:26|2008-01-01 18:14:40|1.1951941761133332E9|
# |    72|       8002545|2008-01-01 16:17:40|2008-01-01 16:19:17|1.1951872754666667E9|
# |   T03|       8014470|2008-01-02 23:04:20|2008-01-02 23:09:20|1.1952979091333334E9|
# |   E13|       8028324|2008-01-04 19:44:02|2008-01-04 19:48:08|1.1954581011933334E9|
# |   M43|       8046485|2008-01-05 13:32:15|2008-01-05 13:45:48|     1.19552254755E9|
# |   T01|       8008096|2008-01-01 23:13:58|2008-01-01 23:16:22|1.1952122172066667E9|
# |   E38|       8041729|2008-01-05 14:17:53|2008-01-05 14:21:25|1.1955246754233334E9|
# |   E01|       8004766|2008-01-01 20:24:07|2008-01-01 20:26:54|1.1952020831766667E9|
# |   B03|       8025510|2008-01-03 00:03:43|2008-01-03 00:08:06|1.1953014232566667E9|
# |   M19|       8010239|2008-01-02 17:55:51|2008-01-02 17:59:36|     1.19527938683E9|
# |   B01|       8028611|2008-01-04 17:59:16|2008-01-04 18:01:05|1.1954516991466668E9|
# +------+--------------+-------------------+-------------------+--------------------+



# 4.4. What were the most common call types?
most_common_calls = fire_df\
                        .groupBy("callType")\
                        .count()\
                        .orderBy("count", ascending=False)\
                        .limit(10)

# most_common_calls.show()

# Output:
# +--------------------+-------+                                                  
# |            callType|  count|
# +--------------------+-------+
# |    Medical Incident|4247943|
# |              Alarms| 720968|
# |      Structure Fire| 714873|
# |   Traffic Collision| 259541|
# |               Other| 110855|
# |Citizen Assist / ...|  96222|
# |        Outside Fire|  85967|
# |        Water Rescue|  34061|
# |Gas Leak (Natural...|  30484|
# |        Vehicle Fire|  28378|
# +--------------------+-------+

# 4.5. What zip codes accounted for the most common calls?
wnd_callType = Window.partitionBy("callType") 
most_common_zipcodes = fire_df\
                        .withColumn("callCount", count("*").over(wnd_callType))\
                        .groupBy("callType", "callCount")\
                        .agg(
                            collect_set("zipcode")
                        )\
                        .orderBy("callCount")

# most_common_zipcodes.show(truncate=False)

#Output:
# +--------------------+---------+--------------------+                           
# |            callType|callCount|collect_set(zipcode)|
# +--------------------+---------+--------------------+
# |      Administrative|      345|[94132, 94124, 94...|
# |  Aircraft Emergency|     1512|[94132, 94121, 94...|
# |              Alarms|   720968|[94132, 94124, 94...|
# |Confined Space / ...|      791|[94132, 94124, 94...|
# |Elevator / Escala...|    17952|[94132, 94124, 94...|
# |Lightning Strike ...|       21|[94118, 94122, 94...|
# |         Marine Fire|      508|[94124, 94111, 94...|
# |Odor (Strange / U...|    13673|[94132, 94124, 94...|
# |Structure Fire / ...|    18894|[94132, 94124, 94...|
# |       Assist Police|     1508|[94132, 94124, 94...|
# |Citizen Assist / ...|    96222|[94132, 94124, 94...|
# |   Electrical Hazard|    21907|[94132, 94124, 94...|
# |           Explosion|     3067|[94132, 94124, 94...|
# |Extrication / Ent...|      935|[94132, 94124, 94...|
# |Gas Leak (Natural...|    30484|[94132, 94124, 94...|
# |              HazMat|     4399|[94132, 94124, 94...|
# |   High Angle Rescue|     1456|[94132, 94124, 94...|
# |           Oil Spill|      518|[94132, 94124, 94...|
# |               Other|   110855|[94132, 94124, 94...|
# |        Outside Fire|    85967|[94132, 94124, 94...|
# +--------------------+---------+--------------------+
# only showing top 20 rows


# 4.6. What San Francisco neighborhoods are in the zip codes 94102 and 94103
neighborhoods_in_San_Francisco = fire_df\
                                    .filter("zipcode IN (94102, 94103)")\
                                    .select("neighborhoods", "zipcode").distinct()

# neighborhoods_in_San_Francisco.show()

# 4.7. What was the sum of all calls, average, min, and max of the call response times?
reponse_aggregates = fire_df\
                        .where(expr("year(callDate) = 2018"))\
                        .withColumn("delay", unix_timestamp("response") - unix_timestamp("received"))\
                        .agg(
                            sum("delay").alias("total_delay"),
                            avg("delay").alias("avg_delay"),
                            min("delay").alias("min_delay"),
                            max("delay").alias("max_delay")
                        )

reponse_aggregates.show()

#Output:
# +-----------+-----------------+---------+---------+                             
# |total_delay|        avg_delay|min_delay|max_delay|
# +-----------+-----------------+---------+---------+
# |   39303453|129.8562224740558|   -86370| 30499374|
# +-----------+-----------------+---------+---------+

# 4.8. How many distinct years of data are in the CSV ﬁle?
distinct_years = fire_df\
                    .select(year("callDate").alias("Year")).distinct().count()

# distinct_years.show()

#Output:
# +----+                                                                          
# |Year|
# +----+
# |2003|
# |2007|
# |2018|
# |2015|
# |2023|
# |2006|
# |2022|
# |2013|
# |2014|
# |2019|
# |2004|
# |2020|
# |2012|
# |2009|
# |2016|
# |2001|
# |2005|
# |2000|
# |2010|
# |2011|
# +----+
# only showing top 20 rows


# 4.9. What week of the year in 2018 had the most ﬁre calls?
week_most_fire_calls_in_2018 = fire_df\
                                    .where(expr("year(callDate) = 2018"))\
                                    .groupBy(weekofyear("callDate").alias("week"))\
                                    .count()\
                                    .orderBy("count", ascending=False)

week_most_fire_calls_in_2018.show()

# Output:
# +----+------+                                                                   
# |week| count|
# +----+------+
# |   1|181900|
# |   2|128800|
# +----+------+


# 4.10. What neighborhoods in San Francisco had the worst response time in 2018?
worst_response_time = fire_df\
                        .where(expr("year(callDate) = 2018"))\
                        .withColumn("delay", unix_timestamp("response") - unix_timestamp("received"))\
                        .groupBy("neighborhoods")\
                        .agg(
                            avg("delay").alias("avgdelay")
                        )\
                        .orderBy("avgdelay", ascending=False)

worst_response_time.show()

#Output:
# +--------------------+------------------+                                       
# |       neighborhoods|          avgdelay|
# +--------------------+------------------+
# |     South of Market|1023.6706147593039|
# |           Glen Park|244.83666666666667|
# |            Seacliff|230.45904761904762|
# |        McLaren Park|214.68802228412255|
# |                None|204.07291666666666|
# |    Golden Gate Park|203.01874633860575|
# |   Lone Mountain/USF|187.34314550042052|
# |     Pacific Heights| 168.9421501421167|
# |           Japantown|167.83761939750184|
# |      Outer Richmond|148.65479219677692|
# |      Inner Richmond|140.13831680650904|
# |           Excelsior|129.30378502375882|
# |         North Beach| 128.3726159094433|
# |      Haight Ashbury|118.49293246578416|
# |           Lakeshore|115.51553767046725|
# |        Potrero Hill|114.58039505261215|
# |   Visitacion Valley|112.74188222344524|
# |    Western Addition| 84.15291076216009|
# |           Chinatown| 71.20536019119153|
# |Bayview Hunters P...| 58.16611078412138|
# +--------------------+------------------+



# 5. For each department, rank employees by their salary in descending order. Include employee name, department name, salary, and their rank within the
# department. Handle ties so employees with the same salary get the same rank, with the next rank(s) skipped.
wnd = Window.partitionBy("deptno").orderBy(desc("sal"))
result5 = emps.withColumn("rank", rank().over(wnd))\
            .join(depts, "deptno")\
            .select("ename", "dname", "sal", "rank")

# result5.show()
# Output:
# +------+----------+-------+----+
# | ename|     dname|    sal|rank|
# +------+----------+-------+----+
# |  KING|ACCOUNTING|5000.00|   1|
# | CLARK|ACCOUNTING|2450.00|   2|
# |MILLER|ACCOUNTING|1300.00|   3|
# | SCOTT|  RESEARCH|3000.00|   1|
# |  FORD|  RESEARCH|3000.00|   1|
# | JONES|  RESEARCH|2975.00|   3|
# | ADAMS|  RESEARCH|1100.00|   4|
# | SMITH|  RESEARCH| 800.00|   5|
# | BLAKE|     SALES|2850.00|   1|
# | ALLEN|     SALES|1600.00|   2|
# |TURNER|     SALES|1500.00|   3|
# |  WARD|     SALES|1250.00|   4|
# |MARTIN|     SALES|1250.00|   4|
# | JAMES|     SALES| 950.00|   6|
# +------+----------+-------+----+


# 6. Salary Diﬀerence from Department Average For each employee, show how much their salary diﬀers from their department's average salary. Also show
# the department's maximum salary. - Output: ename, dname, sal, dept_avg_sal, sal_diﬀ_from_avg, dept_max_sal
wnd = Window.partitionBy("deptno")
result6 = emps.withColumn("dept_avg_sal", avg("sal").over(wnd))\
            .withColumn("sal_diff_from_avg", expr("dept_avg_sal - sal"))\
            .withColumn("dept_max_sal", max("sal").over(wnd))\
            .join(depts, "deptno")\
            .select("ename", "dname", "sal", "dept_avg_sal", "sal_diff_from_avg", "dept_max_sal")

# result6.show()

# Output:
# +------+----------+-------+------------+-----------------+------------+         
# | ename|     dname|    sal|dept_avg_sal|sal_diff_from_avg|dept_max_sal|
# +------+----------+-------+------------+-----------------+------------+
# | CLARK|ACCOUNTING|2450.00| 2916.666667|       466.666667|     5000.00|
# |  KING|ACCOUNTING|5000.00| 2916.666667|     -2083.333333|     5000.00|
# |MILLER|ACCOUNTING|1300.00| 2916.666667|      1616.666667|     5000.00|
# | SMITH|  RESEARCH| 800.00| 2175.000000|      1375.000000|     3000.00|
# | JONES|  RESEARCH|2975.00| 2175.000000|      -800.000000|     3000.00|
# | SCOTT|  RESEARCH|3000.00| 2175.000000|      -825.000000|     3000.00|
# | ADAMS|  RESEARCH|1100.00| 2175.000000|      1075.000000|     3000.00|
# |  FORD|  RESEARCH|3000.00| 2175.000000|      -825.000000|     3000.00|
# | ALLEN|     SALES|1600.00| 1566.666667|       -33.333333|     2850.00|
# |  WARD|     SALES|1250.00| 1566.666667|       316.666667|     2850.00|
# |MARTIN|     SALES|1250.00| 1566.666667|       316.666667|     2850.00|
# | BLAKE|     SALES|2850.00| 1566.666667|     -1283.333333|     2850.00|
# |TURNER|     SALES|1500.00| 1566.666667|        66.666667|     2850.00|
# | JAMES|     SALES| 950.00| 1566.666667|       616.666667|     2850.00|
# +------+----------+-------+------------+-----------------+------------+

# 7. For each department, identify:
# The ﬁrst employee hired (earliest hire date)
# The last employee hired (most recent hire date)
# The employee hired immediately after each employee (by hire date)
# Output: deptno, dname, ename, hire, ﬁrst_hire_ﬂag, last_hire_ﬂag, next_hire_employee

wnd_dept = Window.partitionBy("deptno")
wnd_hire = Window.orderBy("hire")

temp_emps = emps.withColumn("next_rnk", dense_rank().over(wnd_hire))\
                .select(col("ename").alias("next_emp_name"), col("hire").alias("next_emp_hire"), col("next_rnk"))\
                .alias("temp")

# result7 = emps.withColumn("first_hire_flag", min("hire").over(wnd_dept))\
#             .withColumn("last_hire_flag", max("hire").over(wnd_dept))\
#             .withColumn("rnk", dense_rank().over(wnd_hire))\
#             .alias("e")\
#             .join(temp_emps, col("e.rnk") + 1 == col("temp.next_rnk"))\
#             .join(depts, "deptno")\
#             .select("e.deptno", "dname", "e.ename", "e.hire", "first_hire_flag", "last_hire_flag", "next_emp_name", "next_emp_hire")

result7 = emps.withColumn("first_hire_flag", min("hire").over(wnd_dept))\
            .withColumn("last_hire_flag", max("hire").over(wnd_dept))\
            .withColumn("next_hire_date", lead("hire").over(wnd_hire))\
            .withColumn("next_hire_date", lead("ename").over(wnd_hire))\
            .join(depts, "deptno")\
            .select("deptno", "dname", "ename", "hire", "first_hire_flag", "last_hire_flag", "next_hire_date")

# result7.show()

# Output:
# +------+----------+------+----------+---------------+--------------+-------------+-------------+
# |deptno|     dname| ename|      hire|first_hire_flag|last_hire_flag|next_emp_name|next_emp_hire|
# +------+----------+------+----------+---------------+--------------+-------------+-------------+
# |    10|ACCOUNTING| CLARK|1981-06-09|     1981-06-09|    1982-01-23|       TURNER|   1981-09-08|
# |    10|ACCOUNTING|  KING|1981-11-17|     1981-06-09|    1982-01-23|         FORD|   1981-12-03|
# |    10|ACCOUNTING|  KING|1981-11-17|     1981-06-09|    1982-01-23|        JAMES|   1981-12-03|
# |    10|ACCOUNTING|MILLER|1982-01-23|     1981-06-09|    1982-01-23|        SCOTT|   1982-12-09|
# |    20|  RESEARCH| SMITH|1980-12-17|     1980-12-17|    1983-01-12|        ALLEN|   1981-02-20|
# |    20|  RESEARCH| JONES|1981-04-02|     1980-12-17|    1983-01-12|        BLAKE|   1981-05-01|
# |    20|  RESEARCH|  FORD|1981-12-03|     1980-12-17|    1983-01-12|       MILLER|   1982-01-23|
# |    20|  RESEARCH| SCOTT|1982-12-09|     1980-12-17|    1983-01-12|        ADAMS|   1983-01-12|
# |    20|  RESEARCH| ADAMS|1983-01-12|     1980-12-17|    1983-01-12|         NULL|         NULL|
# |    30|     SALES| ALLEN|1981-02-20|     1981-02-20|    1981-12-03|         WARD|   1981-02-22|
# |    30|     SALES|  WARD|1981-02-22|     1981-02-20|    1981-12-03|        JONES|   1981-04-02|
# |    30|     SALES| BLAKE|1981-05-01|     1981-02-20|    1981-12-03|        CLARK|   1981-06-09|
# |    30|     SALES|TURNER|1981-09-08|     1981-02-20|    1981-12-03|       MARTIN|   1981-09-28|
# |    30|     SALES|MARTIN|1981-09-28|     1981-02-20|    1981-12-03|         KING|   1981-11-17|
# |    30|     SALES| JAMES|1981-12-03|     1981-02-20|    1981-12-03|       MILLER|   1982-01-23|
# +------+----------+------+----------+---------------+--------------+-------------+-------------+


# Close session
spark.stop()


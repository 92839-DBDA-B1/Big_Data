#4. Load Fire Service Calls with pre-deﬁned schema. Repeat all 10 Hive assignments on that dataset. Do the assignments using Dataframe syntax. Use Linux
#command below to create sample dataset.


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .appName("Q_4")\
                  .getOrCreate()

fire_schema="/home/sunbeam/Desktop/SUNBEAM/Big/BigData/data/Fire_Department_Calls_for_Service.csv"
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
                .csv(fire_schema)

fire_data.printSchema()
#fire_data.show(n=10)

fire_df=fire_data\
                .withColumn("callDate",to_date("callDate","MM/dd/yyyy"))\
                .withColumn("watchDate",to_date("watchDate","MM/dd/yyyy"))\
                .withColumn("received",to_timestamp("entry","MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("entry",to_timestamp("entry","MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("dispatch",to_timestamp("dispatch","MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("response",to_timestamp("response","MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("onScene",to_timestamp("onScene","MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("onScene", to_timestamp("onScene", "MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("transport", to_timestamp("transport", "MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("hospital", to_timestamp("hospital", "MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("availableTime", to_timestamp("availableTime", "MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("dataAsOf", to_timestamp("dataAsOf", "MM/dd/yyyy hh:mm:ss a"))\
                .withColumn("dataLoadedAt", to_timestamp("dataLoadedAt", "MM/dd/yyyy hh:mm:ss a"))\
                
fire_df.show(n=10)

call_type=fire_data.select("callType").distinct()
call_type.show()

delayed_responses=fire_df.withColumn("delay",unix_timestamp("response")-unix_timestamp("received")/300).filter("delay > 5")\
                            .select("unitId", "incidentNumber", "received", "response", "delay")

delayed_responses.show()

most_common_calls=fire_df\
                        .groupBy("callType")\
                        .count()\
                        .orderBy("count",ascending=False)\
                        .limit(10)
most_common_calls.show()

spark.stop()

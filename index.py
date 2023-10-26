#USING APACHE SPARK 
from pyspark.sql import SparkSession #==== Create a spark session 
spark = SparkSession.builder.appName('Calllogs').getOrCreate()

#========STARTING WITH THE FIRST DATASET 'CALL LOG'
df_call_logs = spark.read.csv('call log.csv', header=True)  #======Read the call log data set
df_call_logs.show(5)  #=======Display data

#======Import modules
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

df_call_logs.printSchema()  #=====View columns and data types

df_call_logs.describe().show()  #====== Get a summary of dataset like max, min and count

df_call_logs.dtypes #======= show data types

#========DATA CLEANING AND TRANSFORMATION

# 1. #Drop duplicate rows
df_call_logs.dropDuplicates()


# 2. Changing data type of 'resolutionDurationInHours' to int
df_call_logs = df_call_logs.withColumn('resolutionDurationInHours', col('resolutionDurationInHours').cast(IntegerType()))


# 3. Filling all null records with 'N/A'
df_call_logs = df_call_logs.fillna({'assignedTo': 'N/A'})


# 4. Transforming all text in 'status' column to sentence case format
df_call_logs =df_call_logs.withColumn('status', F.when(F.lower('status')== 'closed', 'Closed').otherwise(df_call_logs.status))

df_call_logs =df_call_logs.withColumn('status', F.when(F.lower('status')== 'new', 'New').otherwise(df_call_logs.status))

df_call_logs =df_call_logs.withColumn('status', F.when(F.lower('status')== 'resolved', 'Resolved').otherwise(df_call_logs.status))

df_call_logs =df_call_logs.withColumn('status', F.when(F.lower('status')== 'pending', 'Pending').otherwise(df_call_logs.status))


# 5. #Rename the columns
renamed_columns = {'callerID':'caller_id'
                  , 'agentID':'agent_id' 
                  ,'complaintTopic': 'complaint_topic'
                  ,'assignedTo':'assigned_to'
                  ,'resolutionDurationInHours': 'resolution_duration_in_hours'}

df_call_logs = df_call_logs.withColumnsRenamed(renamed_columns)

df_call_logs.show()
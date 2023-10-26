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


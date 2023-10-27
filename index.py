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

#======Data exploration
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



#=======Working on the second dataset 'call details'
spark = SparkSession.builder.appName('Calldetails').getOrCreate()

df_call_details = spark.read.csv('call details.csv', header=True) # Read the file

#========Data exploration
df_call_details.show()  #====show dataset

df_call_details.printSchema() #====show columns and datatypes

df_call_details.describe().show() #========gives a summary of data set like total count, max and avg

df_call_details.dtypes #=======shows data types


#========DATA CLEANING AND TRANSFORMATION
df_call_details.dropDuplicates()

#=====changing data type of callDurationInSeconds column to integer
df_call_details = df_call_details.withColumn('callDurationInSeconds', col('callDurationInSeconds').cast(IntegerType()))

#=====Correcting error of value in first row
df_call_details= df_call_details.na.replace('ageentsGradeLevel', '1')

#======= Cleaning column values of call_type to similar sentence case format
df_call_details = df_call_details.withColumn('calltype', F.when(F.lower('calltype')== 'inbound', 'Inbound').otherwise(df_call_details.calltype))

df_call_details = df_call_details.withColumn('calltype', regexp_replace('calltype', 'in-bound', 'Inbound'))

df_call_details = df_call_details.withColumn('callendedbyagent', regexp_replace('callendedbyagent', 'TRUE', 'True'))

df_call_details = df_call_details.withColumn('callendedbyagent', regexp_replace('callendedbyagent', 'FALSE', 'False'))

#==========Renaming columns to an easily readable format
renamed_columns2 = {'callID':'call_id', 'callDurationInSeconds' : 'call_duration_in_seconds', 'agentsGradeLevel': 'agents_grade_level', 'callType':'call_type', 'callEndedByAgent': 'call_ended_by_agent'}

df_call_details = df_call_details.withColumnsRenamed(renamed_columns2)


#========== Writing transformed files to csv

df_call_logs.write.csv('transformed_call_logs', header = True)

df_call_details.write.csv('transformed_call_details', header = True)


## EXPORT TRANSFORMED FILES TO POSTGRESQL
# Create a database 'weserve' in postgresql

config = configparser.ConfigParser()
config.read('.env')

from utils.helper import create_dbconnection
from sql_statements.create import call_log, call_details

['DB_CONN']
host = config['DB_CONN']['host']
user = config['DB_CONN']['user']
password = config['DB_CONN']['password']
database = config['DB_CONN']['database']


#=========Create a connection to the WESERVE database in Postgresql
conn = create_dbconnection(host,user,password,database)

#=========Create tables in the database
cursor = conn.cursor()  # create cursor connection

cursor.execute(call_log) #======call_log table
conn.commit()

cursor.execute(call_details)#======call_details table
conn.commit()

cursor.close()  #======close cursor
conn.close()   #=====close connection


#========== Use sqlalchemy to export data to weserve database
import sqlalchemy as sa
import pandas as pd
from sqlalchemy import create_engine

#===== create connection to database
engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')


df = pd.read_csv('call_logs.csv') #======create call_log dataframe  
df.to_sql('call_log', engine, if_exists='append', index = False)  #=====export to postgresql


df = pd.read_csv('call_details.csv') #========create call_details dataframe
df.to_sql('call_details', engine, if_exists='append', index = False) #=====export to postgresql




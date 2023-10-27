import boto3
import configparser 
import pandas as pd
import logging 
import redshift_connector

config = configparser.ConfigParser()
config.read('.env')


from utils.helper import create_bucket
from sqlalchemy import create_engine
from sql_statements.create import raw_data_tables, transformed_tables
from sql_statements.transform import transformed_queries



['AWS']
access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
bucket_name = config['AWS']['bucket_name']
region = config['AWS']['region']
role = config['AWS']['arn']

['DB_CONN']
db_host = config['DB_CONN']['host']
db_user = config['DB_CONN']['user']
db_password = config['DB_CONN']['password']
db_database = config['DB_CONN']['database']

['DWH_CONN']
dwh_host = config['DWH_CONN']['host']
dwh_user = config['DWH_CONN']['user']
dwh_password = config['DWH_CONN']['password']
dwh_database = config['DWH_CONN']['database']


#============ CREATE S3 BUCKET (DATA LAKE)
create_bucket(access_key, secret_key, bucket_name, region)


#============ Create connection from postgres to data lake
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_database}")
print('conection')

s3_path = 's3://{}/{}.csv'
for table in raw_data_tables:
    query = f'SELECT * FROM {table}'
    df = pd.read_sql_query(query, engine) #===read the data using pandas
    print(f'=========================Executing{query[:200]}')
    df.to_csv(                            #===write to s3 bucket
        s3_path.format(bucket_name, table)
        ,index = False
        , storage_options= {
            'key': access_key
            ,'secret': secret_key
        }
    )


#========================== Create connection to data warehouse using redshift_connector

conn_details = {'host':dwh_host, 'database': dwh_database, 'user':dwh_user, 'password': dwh_password}
dwh_conn = redshift_connector.connect(**conn_details)
print('connection succesful')

cursor = dwh_conn.cursor()

# #========================== Create the dev schema
dev_schema = 'raw_data'
create_dev_schema = f'CREATE SCHEMA {dev_schema};'
cursor.execute(create_dev_schema)
dwh_conn.commit()
print('created')


#========================== Create the tables for the database in the dev schema
for query in raw_data_tables:
    print(f'=================================={query[:300]}')
    cursor.execute(query)
    dwh_conn.commit()

#========================Copy the data from s3 into the s3 bucket
for table in raw_data_tables:
    cursor.execute(f'''
                COPY {dev_schema}.{table}
                FROM 's3_path.format(bucket_name,table)
                IAM_ROLE '{role}
                DELIMITER ','
                IGNOREHEADER 1;
    ''')
    dwh_conn.commit()
    

#========================== Create transformed/staging schema
transformed_schema = 'staging'
create_staging_schema = f'CREATE SCHEMA {transformed_schema};'
cursor.execute(create_staging_schema)
dwh_conn.commit()


#===========================Create the tables for the staging schema
for query in transformed_tables:
    print(f'==================== Executing{query[:300]}')
    cursor.execute(query)
    dwh_conn.commit()


#==============================Load data into staging schema
for query in transformed_queries:
    logging.info(f'========================{query[:500]}')
    cursor.execute(query)
    dwh_conn.commit()


#=============================DATA QUALITY CHECK
query = 'SELECT COUNT(*) FROM staging.dim_resolution_time_rank'

result = cursor.fetchall()
print(result)

cursor.close()
dwh_conn.close()

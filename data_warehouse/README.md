# data_pipeline_with_aws
Data transformation and pipeline creation with apache spark and cloud warehousing


- About the Business
WeServe is a call service agency that outsources customer service personnels to several companies.
These agents are located in one call center. They receive calls from customers, and make calls to customers, in order to listen to complaints, and get feedback of products and services they have purchased from varying companies. These calls are recorded in the call log sheet, with some extra details saved in the
call details sheet. The customer service managers would like to understand the activities of these
call agents better. Hence, you have been provided with a sample of the sheets you need.


- TASKS
1. Understand the dataset
2. Clean & Transform the dataset into a structure suitable for a data warehouse, and for
analysis
3. Create aggregates, among facts and dim schemas, that will help with KPI
measurements about the business.


- TOOLS  
1. Python - Chosen due to its adaptibility, flexibility and varied libraries for data handling  
2. Apache Spark - Chosen because of its speed in handling large datasets, scalability, rich ecosystem like spark sql. I used it a my main tool in the cleaning and transformation tasks.
3. Pandas - I used this to create a dataframe and also for exporting my dataset to postgresql.
4. Psycopg2 - Used to create a connection to the database in postgresql to create my tables in the database.
5. Sqlalchemy - I used this to create a connection postgresql and export my transformed datasets.
6. Configparser - Used to read configuration files and sensitive data
7. boto3 and s3fs -Python libraries used to access AWS services like creating a bucket
8. Redshift connector. Used to connect and interact with Amazon Redshift




dev_schema = 'raw_data'

#----------- Creating the tables for DEV schema(data lake)

calllog = '''CREATE TABLE IF NOT EXISTS raw_data.calllog
(
	id VARCHAR PRIMARY KEY NOT NULL
	, caller_id VARCHAR
	, agent_id VARCHAR	
	, complaint_topic VARCHAR
 	, assigned_to VARCHAR
  	, status VARCHAR
 	, resolution_duration_in_hours NUMERIC(10, 2)
);'''


call_details = '''CREATE TABLE IF NOT EXISTS raw_data.call_details(
    id VARCHAR PRIMARY KEY NOT NULL
    ,call_id VARCHAR 
    , call_duration_in_seconds NUMERIC (10, 2)
    , agents_grade_level VARCHAR 
    , call_type VARCHAR
    , call_ended_by_agent VARCHAR 
    , FOREIGN KEY (call_id) REFERENCES raw_data.call_log(id)
);'''

raw_data_tables = ['calllog', 'call_details']





#---------Creating the tables for STAR schema (data warehouse)


dim_call_log = '''CREATE TABLE IF NOT EXISTS staging.dim_call_log(
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL
        , call_log_id VARCHAR
        , caller_id VARCHAR
        , agent_id VARCHAR	
        , complaint_topic VARCHAR
        , assigned_to VARCHAR
        , status VARCHAR
    );'''


dim_call_details = '''CREATE TABLE IF NOT EXISTS staging.dim_call_details(
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL
        , call_details_id VARCHAR
        , call_id VARCHAR
        , agent_grade_level VARCHAR
        , call_type VARCHAR
        , call_ended_by_agent VARCHAR 
);'''


dim_call_duration = ''' CREATE TABLE IF NOT EXISTS staging.dim_call_duration( 
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL 
        , agent_id VARCHAR
        , agent_grade_level VARCHAR
        , avg_call_duration BIGINT
        , total_call_duration BIGINT
 );'''


dim_call_resolution_status = '''CREATE TABLE IF NOT EXISTS staging.dim_call_resolution_status(
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL 
        , agent_id VARCHAR
        , calls_received BIGINT
        , calls_assigned BIGINT
        , calls_resolved BIGINT
);'''


dim_resolution_time_rank = '''CREATE TABLE IF NOT EXISTS staging.dim_resolution_time_rank(
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL 
        , agent_id VARCHAR
        , agent_grade_level VARCHAR
        , resolution_duration_in_hours BIGINT
        , resolution_time_rank BIGINT
);'''


ft_call_transactions = '''CREATE TABLE IF NOT EXISTS staging.ft_call_transactions(
        id BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL
        , call_log_id BIGINT
        , call_details_id BIGINT
        , call_duration_id BIGINT
        , calls_resolution_status_id BIGINT
        , resolution_time_rank BIGINT
        , resolution_duration_in_hours BIGINT
        , call_duration_in_seconds BIGINT
        , FOREIGN KEY (call_log_id) REFERENCES dim_call_log (id)
        , FOREIGN KEY (call_details_id) REFERENCES dim_call_details (id)
        , FOREIGN KEY (call_duration_id) REFERENCES dim_call_duration (id)
        , FOREIGN KEY (call_resolution_status_id) REFERENCES dim_call_resolution_status (id)
);'''


transformed_tables = ['dim_call_log', 'dim_call_details', 'dim_call_duration', 'dim_call_resolution_status', 'dim_resolution_time_rank', 'ft_call_transactions']
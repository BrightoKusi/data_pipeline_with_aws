#----------- Creating the raw table for the database in postgresql

call_log = '''CREATE TABLE IF NOT EXISTS call_log(
	id VARCHAR PRIMARY KEY
	,caller_id VARCHAR
	, agent_id VARCHAR	
	, complaint_topic VARCHAR
 	, assigned_to VARCHAR
  	, status VARCHAR
 	, resolution_duration_in_hours NUMERIC
);'''


#==== Generating an auto-incrementing column to serve as Primary key so that 'call_id' column will be the foreign key to form a relationship between the two tables
call_details = '''CREATE TABLE IF NOT EXISTS call_details(
    id serial PRIMARY KEY
    ,call_id VARCHAR REFERENCES call_log(id)
    , call_duration_in_seconds numeric
    , agents_grade_level VARCHAR 
    , call_type VARCHAR
    , call_ended_by_agent VARCHAR 
);'''



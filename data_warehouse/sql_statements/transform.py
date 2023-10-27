
dim_call_log = '''INSERT INTO staging.dim_call_log (
        call_log_id
        , caller_id
        , agent_id
        , complaint_topic
        , assigned_to
        , status 
    )
        SELECT cl.id, cl.caller_id, cl.agent_id, cl.complaint_topic, cl.assigned_to, cl.status
        FROM raw_data.call_log cl
    ;'''

dim_call_details = '''INSERT INTO staging.dim_call_details (
        call_details_id
        , call_id
        , agents_grade_level
        , call_type
        , call_ended_by_agent
    )
        SELECT cd.id, cd.call_id, cd.agents_grade_level, cd.call_type, cd.call_ended_by_agent
        FROM raw_data.call_details cd
    ;'''

dim_call_duration = '''INSERT INTO staging.dim_call_duration (
        agent_id,
        agents_grade_level,
        avg_call_duration,
        total_call_duration
    )
        SELECT DISTINCT
            cl.agent_id, cd.agents_grade_level,
            AVG(cd.call_duration_in_seconds) OVER (PARTITION BY cl.agent_id) AS avg_call_duration,
            SUM(cd.call_duration_in_seconds) OVER (PARTITION BY cl.agent_id) AS total_call_duration
        FROM raw_data.call_log cl
        LEFT JOIN raw_data.call_details cd 
        ON cl.id = cd.call_id
    ;'''


dim_call_resolution_status = '''INSERT INTO staging.dim_call_resolution_status (
        agent_id,
        calls_received,
        calls_assigned,
        calls_resolved
        )
    SELECT
        cl.agent_id,
        COUNT(cl.agent_id) OVER (PARTITION BY cl.agent_id ORDER BY cl.agent_id DESC) AS calls_received,
        COUNT(cl.assigned_to) OVER (PARTITION BY cl.agent_id ORDER BY cl.agent_id DESC) AS calls_assigned,
        SUM(CASE WHEN cl.status = 'Resolved' THEN 1 ELSE 0 END) OVER (PARTITION BY cl.agent_id ORDER BY cl.agent_id DESC) AS calls_resolved
    FROM raw_data.call_log cl;
    ;'''



dim_resolution_time_rank = '''INSERT INTO staging.dim_resolution_time_rank(
        agent_id
        , agent_grade_level
        , resolution_duration_in_hours
        , resolution_time_rank
    )
    SELECT cl.id, cd.agents_grade_level, cl.resolution_duration_in_hours
    , RANK() OVER (ORDER BY cl.resolution_duration_in_hours DESC) AS resolution_time_rank
    FROM raw_data.call_log cl
    LEFT JOIN raw_data.call_details cd
        ON cl.id = cd.call_id 
;'''
        



ft_call_transactions = '''INSERT INTO staging.ft_call_transactions (
        call_log_id,
        call_details_id,
        call_duration_id,
        call_resolution_status_id,
        resolution_time_rank_id,
        resolution_duration_in_hours,
        call_duration_in_seconds
        )
    SELECT
        cl.id AS call_log_id,
        cd.id AS call_details_id,
        cdu.id AS call_duration_id,
        crs.id AS call_resolution_status_id,
        rtr.id AS resolution_time_rank_id,
        cl.resolution_duration_in_hours,
        cd.call_duration_in_seconds
    FROM staging.dim_call_log cl
    JOIN staging.dim_call_details cd ON cl.call_id = cd.call_id
    JOIN staging.dim_call_duration cdu ON cl.call_id = cdu.call_id
    JOIN staging.dim_call_resolution_status crs ON cl.agent_id = crs.agent_id
    JOIN staging.dim_resolution_time_rank rtr ON cl.agent_id = rtr.agent_id
;'''

transformed_queries = ['dim_call_log', 'dim_call_details', 'dim_call_duration', 'dim_call_resolution_status', 'dim_resolution_time_rank', 'ft_call_transactions']

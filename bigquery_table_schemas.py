from google.cloud.bigquery import SchemaField

bigquery_raw_journey_analysis_schema = [
    SchemaField('use_case_engl', 'STRING', 'NULLABLE'),
    SchemaField('coach_name', 'STRING', 'NULLABLE'),
    SchemaField('completed', 'STRING', 'NULLABLE'),
    SchemaField('nps_coach', 'INTEGER', 'NULLABLE'),
    SchemaField('nps_power_coaching', 'INTEGER', 'NULLABLE')
]

bigquery_journey_analysis_topic_schema = [
    SchemaField('use_case_engl', 'STRING', mode='NULLABLE'),
    SchemaField('booked', 'INTEGER', mode='NULLABLE'),
    SchemaField('journey_completed', 'INTEGER', mode='NULLABLE'),
    SchemaField('nps_sparrks_coaching', 'FLOAT', mode='NULLABLE'),
    SchemaField('nps_coach', 'FLOAT', mode='NULLABLE'),
    SchemaField('feedback_n', 'INTEGER', mode='NULLABLE'),
    SchemaField('feedback_p', 'FLOAT', mode='NULLABLE'),
    SchemaField('origin', 'STRING', mode='NULLABLE'),
]

bigquery_journey_analysis_coach_schema = [
    SchemaField('coach_name', 'STRING', mode='NULLABLE'),
    SchemaField('booked', 'INTEGER', mode='NULLABLE'),
    SchemaField('journey_completed', 'INTEGER', mode='NULLABLE'),
    SchemaField('nps_sparrks_coaching', 'FLOAT', mode='NULLABLE'),
    SchemaField('nps_coach', 'FLOAT', mode='NULLABLE'),
    SchemaField('feedback_n', 'INTEGER', mode='NULLABLE'),
    SchemaField('feedback_p', 'FLOAT', mode='NULLABLE'),
    SchemaField('origin', 'STRING', mode='NULLABLE'),
]
from google.cloud.bigquery import SchemaField

from shared_functions import create_bigquery_table

bigquery_raw_journey_analysis_schema = [
    SchemaField('use_case', 'STRING', 'NULLABLE', None, ()),
    SchemaField('coach_first_name', 'STRING', 'NULLABLE', None, ()),
    SchemaField('coach_last_name', 'STRING', 'NULLABLE', None, ()),
    # SchemaField('completed', 'STRING', 'NULLABLE', None, ()),
    # SchemaField('nps_coach', 'INTEGER', 'NULLABLE', None, ()),
    # SchemaField('nps_power_coaching', 'INTEGER', 'NULLABLE', None, ())
]

bigquery_journey_analysis_topic_schema = [
    SchemaField('use_case_engl', 'STRING', 'NULLABLE', None, ()),
    SchemaField('booked', 'STRING', 'NULLABLE', None, ()),
    SchemaField('journey_completed', 'STRING', 'NULLABLE', None, ()),
    SchemaField('nps_sparrks_coaching', 'FLOAT', 'NULLABLE', None, ()),
    SchemaField('nps_coach', 'FLOAT', 'NULLABLE', None, ()),
    SchemaField('feedback_n', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('feedback_percent', 'FLOAT', 'NULLABLE', None, ()),
]

bigquery_journey_analysis_coach_schema = [
    SchemaField('coach_name', 'STRING', 'NULLABLE', None, ()),
    SchemaField('booked', 'STRING', 'NULLABLE', None, ()),
    SchemaField('journey_completed', 'STRING', 'NULLABLE', None, ()),
    SchemaField('nps_sparrks_coaching', 'FLOAT', 'NULLABLE', None, ()),
    SchemaField('nps_coach', 'FLOAT', 'NULLABLE', None, ()),
    SchemaField('feedback_n', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('feedback_percent', 'FLOAT', 'NULLABLE', None, ()),
]
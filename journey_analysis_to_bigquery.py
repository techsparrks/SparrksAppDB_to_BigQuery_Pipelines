import gspread
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

from create_bigquery_table import bigquery_raw_journey_analysis_schema, \
    bigquery_journey_analysis_coach_schema, bigquery_journey_analysis_topic_schema
from journey_analysis_functions import calc_nps_and_feedback, get_journey_analysis_data
from shared_functions import create_bigquery_table, write_data_to_bigquery

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('data-analytics-359712-c05ab5a3dc2a.json', scope)
gs_client = gspread.authorize(creds)

PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'googlesheet_raw_data'

credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-c05ab5a3dc2a.json')
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'googlesheet_raw_data'

sales_funnel_doc_name = 'Moji-SPARRKS Analysis'
journey_analysis_sheet_name = 'Journey analysis'

# define table names
bigquery_journey_analysis_raw_data_table_name = 'journey_analysis_raw'
bigquery_journey_analysis_topic_table_name = 'journey_analysis_topic'
bigquery_journey_analysis_coach_table_name = 'journey_analysis_coach'

# create table for raw NPS data in BigQuery
raw_topic_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_raw_data_table_name)
# create_bigquery_table(client, raw_topic_table_id, bigquery_raw_journey_analysis_schema)

# dict with reference names for the columns from Google Sheets to BigQuery
reference_names = {
    'Use Case engl.': 'use_case_engl',
    'Coach Last Name': 'coach_name',
    'Completed clean': 'completed',
    'NPS Power Coaching': 'nps_power_coaching',
    'NPS Coach': 'nps_coach'
}

# get relevant columns from the Google Sheet for the NPS analysis
nps_columns = ['Use Case engl.', 'Coach Last Name', 'Completed clean', 'NPS Power Coaching', 'NPS Coach']


if __name__ == '__main__':
    # get journey analysis data
    raw_journey_analysis_df = get_journey_analysis_data(gs_client, sales_funnel_doc_name, journey_analysis_sheet_name,
                                                        nps_columns, reference_names)

    # write NPS relevant raw data to BigQuery
    # write_data_to_bigquery(raw_journey_analysis_df, client, bigquery_raw_journey_analysis_schema, raw_topic_table_id,
    #                        bigquery_journey_analysis_raw_data_table_name)

    # calculate NPS and feedback for coaches and topics
    final_ratings_topic_df = calc_nps_and_feedback(raw_journey_analysis_df, 'topic')
    final_ratings_coach_df = calc_nps_and_feedback(raw_journey_analysis_df, 'coach')

    # create tables in BigQuery for coaches and topics NPS analysis
    topic_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_topic_table_name)
    coach_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_coach_table_name)
    create_bigquery_table(client, topic_table_id, bigquery_journey_analysis_topic_schema)
    create_bigquery_table(client, coach_table_id, bigquery_journey_analysis_coach_schema)

    # write NPS analysis to BigQuery
    write_data_to_bigquery(final_ratings_topic_df, client, bigquery_journey_analysis_topic_schema, topic_table_id,
                           bigquery_journey_analysis_topic_table_name)
    write_data_to_bigquery(final_ratings_coach_df, client, bigquery_journey_analysis_coach_schema, coach_table_id,
                           bigquery_journey_analysis_coach_table_name)

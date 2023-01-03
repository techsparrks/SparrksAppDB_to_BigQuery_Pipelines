import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

from bigquery_table_schemas import bigquery_raw_journey_analysis_schema, \
    bigquery_journey_analysis_coach_schema, bigquery_journey_analysis_topic_schema
from journey_analysis_functions import calc_nps_and_feedback, get_journey_analysis_data
from shared_functions import create_bigquery_table, write_data_to_bigquery

PATH = '/home/mysql_to_bigquery_piplines/'
# credentials to access Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(PATH + 'data-analytics-359712-c05ab5a3dc2a.json', scope)
gs_client = gspread.authorize(creds)

# Google Sheet document and sheet name to read data from
sales_funnel_doc_name = 'SPARRKS Analysis'
journey_analysis_sheet_name = 'Journey analysis'

# BigQuery project and database id
PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'old_system_raw_data'

# get BigQuery credentials and connect client
credentials = service_account.Credentials.from_service_account_file(
    PATH + 'data-analytics-359712-c05ab5a3dc2a.json')
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# BigQuery table names to write data to
bigquery_journey_analysis_raw_data_table_name = 'journey_analysis_raw'
bigquery_journey_analysis_topic_table_name = 'journey_analysis_topic'
bigquery_journey_analysis_coach_table_name = 'journey_analysis_coach'

# create table for raw NPS data in BigQuery
raw_topic_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_raw_data_table_name)
# create_bigquery_table(client, raw_topic_table_id, bigquery_raw_journey_analysis_schema)

# get relevant columns from the Google Sheet for the NPS analysis
nps_columns = ['Use Case engl.', 'Coach Last Name', 'Completed', 'NPS Power Coaching', 'NPS Coach']

# dict with reference names for the columns from Google Sheets to BigQuery
reference_names = {
    'Use Case engl.': 'use_case_engl',
    'Coach Last Name': 'coach_name',
    'Completed': 'completed',
    'NPS Power Coaching': 'nps_power_coaching',
    'NPS Coach': 'nps_coach'
}

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
    create_bigquery_table(client, topic_table_id, bigquery_journey_analysis_topic_schema, recreate_table=False)
    create_bigquery_table(client, coach_table_id, bigquery_journey_analysis_coach_schema, recreate_table=False)

    # write NPS analysis to BigQuery
    write_data_to_bigquery(final_ratings_topic_df, client, bigquery_journey_analysis_topic_schema, topic_table_id)
    write_data_to_bigquery(final_ratings_coach_df, client, bigquery_journey_analysis_coach_schema, coach_table_id)

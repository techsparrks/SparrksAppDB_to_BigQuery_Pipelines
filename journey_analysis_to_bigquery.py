import datetime

import gspread
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

from create_bigquery_table import bigquery_raw_journey_analysis_schema, \
    bigquery_journey_analysis_coach_schema, bigquery_journey_analysis_topic_schema
from journey_analysis_functions import calculate_nps_sparrks_coaching, calc_feedback
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

bigquery_journey_analysis_raw_data_table_name = 'journey_analysis_raw'
bigquery_journey_analysis_topic_table_name = 'journey_analysis_topic'
bigquery_journey_analysis_coach_table_name = 'journey_analysis_coach'

raw_topic_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_raw_data_table_name)
create_bigquery_table(client, raw_topic_table_id, bigquery_raw_journey_analysis_schema)

reference_names = {
    'Use Case engl.': 'use_case_engl',
    'Coach Last Name': 'coach_name',
    'Completed clean': 'completed',
    'NPS Power Coaching': 'nps_power_coaching',
    'NPS Coach': 'nps_coach'
}

# get journey analysis data
doc = gs_client.open(sales_funnel_doc_name)
journey_analysis_sheet_read = doc.worksheet(journey_analysis_sheet_name)
journey_analysis_df = pd.DataFrame(journey_analysis_sheet_read.get_values())
journey_analysis_df.columns = journey_analysis_df.iloc[0]
journey_analysis_df = journey_analysis_df[1:]
# journey_analysis_df.replace('', np.nan, inplace=True)

nps_topic_columns = ['Use Case engl.', 'Coach Last Name', 'Completed clean', 'NPS Power Coaching', 'NPS Coach']

journey_analysis_df.replace('', np.nan, inplace=True)
raw_journey_analysis_df = journey_analysis_df[nps_topic_columns].dropna()
raw_journey_analysis_df = raw_journey_analysis_df.rename(columns=reference_names)

write_data_to_bigquery(raw_journey_analysis_df, client, bigquery_raw_journey_analysis_schema, raw_topic_table_id,
                       bigquery_journey_analysis_raw_data_table_name)

# raw_journey_analysis_df['booked'] = raw_journey_analysis_df.groupby('use_case_engl')['use_case_engl'].transform(
#     'count')
# journey_analysis_coach_df['booked'] = journey_analysis_coach_df.groupby('coach_name')['coach_name'].transform('count')

# journey_analysis_topic_df['journey_completed'] = journey_analysis_topic_df.groupby(['use_case_engl', 'completed'])['use_case_engl'].transform('count')
# raw_journey_analysis_df['journey_completed'] = \
# raw_journey_analysis_df[raw_journey_analysis_df['completed'] == 'x'].groupby('use_case_engl')[
#     'use_case_engl'].transform('count')
# journey_analysis_coach_df['journey_completed'] = \
# journey_analysis_coach_df[journey_analysis_coach_df['completed'] == 'x'].groupby('coach_name')['coach_name'].transform(
#     'count')

# raw_journey_analysis_df = raw_journey_analysis_df[raw_journey_analysis_df['use_case_engl'] != '']
# journey_analysis_coach_df = journey_analysis_coach_df[journey_analysis_coach_df['coach_name'] != '']


nps_power_coaching_ratings_topic_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, 'topic', 'nps_power_coaching')
nps_power_coaching_ratings_coach_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, 'coach', 'nps_power_coaching')

nps_coach_ratings_topic_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, 'topic', 'nps_coach')
nps_coach_ratings_coach_df = calculate_nps_sparrks_coaching(raw_journey_analysis_df, 'coach', 'nps_coach')

feedback_n_topic_df = calc_feedback(raw_journey_analysis_df, 'topic')
feedback_n_coach_df = calc_feedback(raw_journey_analysis_df, 'coach')

ratings_topic_df = pd.merge(nps_power_coaching_ratings_topic_df, nps_coach_ratings_topic_df, how='outer')
ratings_coach_df = pd.merge(nps_power_coaching_ratings_coach_df, nps_coach_ratings_coach_df, how='outer')

final_ratings_topic_df = pd.merge(ratings_topic_df, feedback_n_topic_df, how='outer')
final_ratings_coach_df = pd.merge(ratings_coach_df, feedback_n_coach_df, how='outer')

final_ratings_topic_df['feedback_percent'] = final_ratings_topic_df['feedback_n'] / final_ratings_topic_df['journey_completed']
final_ratings_coach_df['feedback_percent'] = final_ratings_coach_df['feedback_n'] / final_ratings_coach_df['journey_completed']


topic_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_topic_table_name)
coach_table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, bigquery_journey_analysis_coach_table_name)
create_bigquery_table(client, topic_table_id, bigquery_journey_analysis_topic_schema)
create_bigquery_table(client, coach_table_id, bigquery_journey_analysis_coach_schema)

write_data_to_bigquery(final_ratings_topic_df, client, bigquery_journey_analysis_topic_schema, topic_table_id,
                       bigquery_journey_analysis_topic_table_name)
write_data_to_bigquery(final_ratings_coach_df, client, bigquery_journey_analysis_coach_schema, coach_table_id,
                       bigquery_journey_analysis_coach_table_name)



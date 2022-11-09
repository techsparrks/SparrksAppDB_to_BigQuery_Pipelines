import datetime

import gspread
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('data-analytics-359712-1f4a4a01f7ba.json', scope)
gs_client = gspread.authorize(creds)


PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'googlesheet_raw_data'

credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-c05ab5a3dc2a.json')
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

sales_funnel_doc_name = 'Test_sheet_to_bigquery'
sales_funnel_sheet_name = 'Session Log'

reference_doc_name = 'GoogleSheets_to_MySQL'
reference_sheet_name = 'Session Log'

# get funnel data
doc = gs_client.open(sales_funnel_doc_name)
sales_funnel_sheet_read = doc.worksheet(sales_funnel_sheet_name)
sales_funnel_df = pd.DataFrame(sales_funnel_sheet_read.get_values())
sales_funnel_df.columns = sales_funnel_df.iloc[0]
sales_funnel_df = sales_funnel_df[1:]
sales_funnel_df.replace('', np.nan, inplace=True)

# get reference data
reference_doc = gs_client.open(reference_doc_name)
reference_sheet_read = reference_doc.worksheet(reference_sheet_name)
reference_df = pd.DataFrame(reference_sheet_read.get_values())
reference_df.columns = reference_df.iloc[0]
reference_df = reference_df[1:]
reference_df = reference_df.set_index(['Session log'])
filtered_reference_df = reference_df[reference_df['coachee_survey'] != '']['coachee_survey']
filtered_reference_df = filtered_reference_df.drop(labels='Topic interest future')
references = filtered_reference_df.to_dict()


get_indices = filtered_reference_df.index.values.tolist()
survey_df = sales_funnel_df[get_indices]
survey_df.dropna(inplace=True)
survey_df.rename(columns=references, inplace=True)
ps = survey_df.rename(columns=references)

rows_to_insert = []
for i, row in ps.iterrows():
    value = row.to_dict()
    value["duration"] = str(datetime.timedelta(seconds=int(value["duration"])))
    value["coachee_future_use"] = True if value["coachee_future_use"] == 'Ja' else False
    rows_to_insert.append(value)


# rows_to_insert = [
#     {"coachee_survey_id": 123, "journey_id": 32, "coachee_id": 123,
#      "survey_date": '2022-10-14T00:00:00', "duration": '00:00:05',
#      "created_at": 	'2022-11-04T10:07:13', "updated_at": 	'2022-11-04T10:07:13'},
#     {"coachee_survey_id": 1234, "journey_id": 332, "coachee_id": 156,
#      "survey_date": '2022-10-15T00:00:00', "duration": '00:00:06',
#      "created_at": '2022-11-04T10:07:14', "updated_at": '2022-11-04T10:07:14'},
# ]

# write data to bigquery

table_name = 'coachee_survey'
table_id = "{}.{}.{}".format(PROJECT_ID, DATABASE_ID, table_name)

# mysql_create_schema_query = get_mysql_table_schema(SQA_CONN_PUB, table_name)
# bigquery_schema_def, _, _, _ = prepare_bigquery_schema(
#     mysql_create_schema_query,
#     table_name)
# create_bigquery_table(client, table_id, bigquery_schema_def)


errors = client.insert_rows_json(table_id, rows_to_insert)
print(errors)



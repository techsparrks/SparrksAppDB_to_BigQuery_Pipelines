import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

from db_config import SQA_CONN_PUB

# credentials to access Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('data-analytics-359712-c05ab5a3dc2a.json', scope)
gs_client = gspread.authorize(creds)


doc_name = 'SPARRKS Analysis'
journey_analysis_sheet_name = 'Journey analysis'
session_log_sheet_name = 'Session log'
opportunities_sheet_name = 'Opportunities'

# get document
doc = gs_client.open(doc_name)

# get sheet to read from
journey_analysis_sheet_read = doc.worksheet(journey_analysis_sheet_name)
journey_analysis_df = pd.DataFrame(journey_analysis_sheet_read.get_values())

session_log_sheet_read = doc.worksheet(session_log_sheet_name)
session_log_df = pd.DataFrame(session_log_sheet_read.get_values())

opportunities_sheet_read = doc.worksheet(opportunities_sheet_name)
opportunities_df = pd.DataFrame(opportunities_sheet_read.get_values())


# set first row as a header for the DataFrame
journey_analysis_df.columns = journey_analysis_df.iloc[0]
journey_analysis_df = journey_analysis_df[1:]

# set first row as a header for the DataFrame
session_log_df.columns = session_log_df.iloc[1]
session_log_df = session_log_df[2:]

# set first row as a header for the DataFrame
opportunities_df.columns = opportunities_df.iloc[0]
opportunities_df = opportunities_df[3:]

journey_analysis_columns = [#'Coach Last Name',
                            'Lifecycle', 'Identifier & Lifecycle',
                            '1. Year nice', 'Session 1 done',
                            '2. Year nice', 'Session 2 done',
                            '3. Year nice', 'Session 3 done',
                            '4. Year nice', 'Session 4 done',
                            'Rescheduled 1'
                            ]
                            #'Rescheduled 2', 'Coach test']

session_log_columns = [
    #'0. Coach last, first',
    '1. Date, start time',
    '2. Date, start time',
    '3. Date, start time',
    '4. Date, start time',
    'Re-scheduling session 1, 3',
    #'Re-scheduling session 2, 4',
    #'Comment coach'
]

opportunities_columns = [
    'Company',
    'Identifier Lifecycle',
    'Rate (45min)'
]

journey_analysis_df = journey_analysis_df[journey_analysis_columns]
session_log_df = session_log_df[session_log_columns]
opportunities_df = opportunities_df[opportunities_columns]

journey_analysis_df.to_sql('journey_analysis_raw', index=False, con=SQA_CONN_PUB, if_exists='replace')
session_log_df.to_sql('session_log_raw', index=False, con=SQA_CONN_PUB, if_exists='replace')
opportunities_df.to_sql('opportunities_raw', index=False, con=SQA_CONN_PUB, if_exists='replace')

SQA_CONN_PUB.close()
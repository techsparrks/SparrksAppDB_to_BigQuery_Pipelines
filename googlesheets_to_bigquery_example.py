import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('data-analytics-359712-1f4a4a01f7ba.json', scope)
client = gspread.authorize(creds)

PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'googlesheet_raw_data'
doc_name = 'Test_sheet_to_bigquery'
sheet_read_name = 'Tabellenblatt1'
sheet_write_name = 'Tabellenblatt2'

# get document
doc = client.open(doc_name)

# get sheet to read from
sheet_read = doc.worksheet(sheet_read_name)

# get sheet to write to
sheet_write = doc.worksheet(sheet_write_name)

# read data from Google Sheets
df = pd.DataFrame(sheet_read.get_all_records())
print(df)

# clear sheet to write to (optional)
sheet_write.clear()

# write to dataframe
set_with_dataframe(worksheet=sheet_write, dataframe=df, include_index=False,
                   include_column_header=True, resize=True)

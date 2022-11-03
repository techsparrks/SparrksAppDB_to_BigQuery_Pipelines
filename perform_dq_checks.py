import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime


from data_quality_check import get_row_count, getSql, runSql

from db_config import SQA_CONN_PUB, SQA_CONN_PUB_ENGINE
from db_queries import get_mysql_table_names_query

credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-c05ab5a3dc2a.json')
PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'sparrksapp_raw_data'

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_mysql_table_names(con):
    table_names_df = pd.read_sql(get_mysql_table_names_query, con=con)
    table_names = table_names_df['TABLE_NAME'].values.tolist()

    return table_names


# Prepare Dynamic SQL Statement
    sqlQuery = getSql(datasetName, dataTable, columnList, rowCount)

    # Run SQL & Display Output
    # runSql(datasetName, dataTable, reportTable, sqlQuery)
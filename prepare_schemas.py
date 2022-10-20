import re

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from sqlalchemy import text

from db_config import SQA_CONN_PUB
from db_queries import get_mysql_table_names_query, get_mysql_table_schemas_query, get_data_from_mysql_query

credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-1f4a4a01f7ba.json')
PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'sparrksapp_raw_data'

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

DATATYPES_DICT = {'int': 'INTEGER',
                  'varchar': 'STRING',
                  'json': 'JSON',
                  'datetime': 'DATETIME',
                  'timestamp': 'TIMESTAMP',
                  'time': 'TIME',
                  'tinyint': 'BOOLEAN',
                  'char': 'STRING',
                  'date': 'DATE',
                  'float': 'NUMERIC'
                  }


def get_mysql_table_names(con):
    table_names_df = pd.read_sql(get_mysql_table_names_query, con=con)
    table_names = table_names_df['TABLE_NAME'].values.tolist()

    return table_names


def get_mysql_table_schema(con, table_name):
    try:
        result = con.execute(text(get_mysql_table_schemas_query.format(table_name)))
        print('Table schema for table', table_name, 'fetched successfully')
    except Exception as e:
        print(e.args[0])
        print('Table schema for table', table_name, 'not fetched.')

    return result.first()[1]


def prepare_bigquery_schema(mysql_schema, table_name):
    schema_def = []
    required_columns = []

    try:
        for line in mysql_schema.splitlines():
            if re.match(r'^\s*`', line):
                column_name = re.findall(r'(`[^"]*)`', line)[0][1:]
                datatype_info = line.split("`", 2)[2].strip().split(' ')[0].split('(')[0]
                if datatype_info in DATATYPES_DICT.keys():
                    datatype = DATATYPES_DICT.get(datatype_info)
                else:
                    datatype = 'STRING'
                if 'NOT NULL' in line:
                    mode_info = 'REQUIRED'
                    required_columns.append(column_name)
                else:
                    mode_info = 'NULLABLE'
                schema_info = bigquery.SchemaField(column_name, datatype, mode_info)
                schema_def.append(schema_info)
        print('Schema for table', table_name, 'created')
    except Exception as e:
        print(e.args[0])
        print('Schema creation for table', table_name, 'failed')

    return schema_def, required_columns


def create_bigquery_table(table_id, bigquery_schema_def):
    try:
        client.get_table(table_id)
        print("Table {} exists already".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=bigquery_schema_def)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}".format(table_id))


def get_data_from_mysql(table_name, con, required_columns):
    try:
        df = pd.read_sql(get_data_from_mysql_query.format(table_name), con=con)

        # handle \r and \n special characters
        df.replace(to_replace=[r"\\n|\\r", "\n|\r"], value=["", ""], regex=True, inplace=True)

        # handle empty strings
        for c in df[required_columns]:
            if '' in df[c].values:
                df[c] = df[c].replace({'': ' '})

        print('Data from table', table_name, 'fetched successfully')
    except Exception as e:
        print(e.args[0])
        print('Getting data from table', table_name, 'failed')

    # con.close()
    # print(df.isna().sum())
    return df


def write_data_to_bigquery(df, schema_def, table_id, table_name):
    # post to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema_def,
                                        write_disposition="WRITE_TRUNCATE",
                                        autodetect=False,
                                        source_format=bigquery.SourceFormat.CSV
                                        )
    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        print('Data for table', table_name, 'uploaded successfully to BigQuery')
    except Exception as e:
        print(e.args[0])
        print('Upload of data for table', table_name, 'to BigQuery failed')


def start_pipeline(project_id, database_id, table_names, con):
    for table_name in table_names:
        mysql_create_schema_query = get_mysql_table_schema(con, table_name)
        schema_def, required_columns = prepare_bigquery_schema(mysql_create_schema_query, table_name)
        table_id = "{}.{}.{}".format(project_id, database_id, table_name)
        create_bigquery_table(table_id, schema_def)
        table_data = get_data_from_mysql(table_name, con, required_columns)
        write_data_to_bigquery(table_data, schema_def, table_id, table_name)
    SQA_CONN_PUB.close()


def one_table(project_id, database_id, con, table_name ='application_area'):
    mysql_create_schema_query = get_mysql_table_schema(con, table_name)
    schema_def, required_columns = prepare_bigquery_schema(mysql_create_schema_query, table_name)
    table_id = "{}.{}.{}".format(project_id, database_id, table_name)
    create_bigquery_table(table_id, schema_def)
    table_data = get_data_from_mysql(table_name, con, required_columns)
    write_data_to_bigquery(table_data, schema_def, table_id, table_name)


table_names_tinyint_bool = ['programmes']
table_names_wrong_duration = ['coach_survey', 'coachee_survey']

start_pipeline(PROJECT_ID, DATABASE_ID, table_names_wrong_duration, SQA_CONN_PUB)

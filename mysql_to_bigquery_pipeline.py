from datetime import datetime
import re

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from sqlalchemy import text

from data_quality_check import get_row_count
from db_config import SQA_CONN_PUB
from db_queries import get_mysql_table_names_query, get_mysql_table_schemas_query, get_data_from_mysql_query

credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-c05ab5a3dc2a.json')
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
    tinyint_columns = []
    time_columns = []

    try:
        for line in mysql_schema.splitlines():
            if re.match(r'^\s*`', line):
                column_name = re.findall(r'(`[^"]*)`', line)[0][1:]
                datatype_info = line.split("`", 2)[2].strip().split(' ')[0].split('(')[0]
                if datatype_info in DATATYPES_DICT.keys():
                    datatype = DATATYPES_DICT.get(datatype_info)
                    if datatype_info == 'tinyint':
                        tinyint_columns.append(column_name)
                    elif datatype_info == 'time':
                        time_columns.append(column_name)
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

    return schema_def, required_columns, tinyint_columns, time_columns


def create_bigquery_table(table_id, bigquery_schema_def):
    try:
        client.get_table(table_id)
        print("Table {} exists already".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=bigquery_schema_def)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}".format(table_id))


def get_raw_data_from_mysql(table_name, con):
    try:
        df = pd.read_sql(get_data_from_mysql_query.format(table_name), con=con)
        print('Raw data from table', table_name, 'fetched successfully')
    except Exception as e:
        df = None
        print(e.args[0])
        print('Getting raw data from table', table_name, 'failed')

    # con.close()
    # print(df.isna().sum())
    return df


def clean_mysql_data(table_name, df, required_columns, tinyint_columns, time_columns):
    error_indices_list = []
    if df is None:
        print('Data frame for table', table_name, 'is None. No cleaning was performed')
        return None
    elif df.empty:
        print('Data frame for table', table_name, 'is empty. No cleaning was performed')
        return df
    else:
        # handle \r and \n special characters
        df.replace(to_replace=[r"\\n|\\r", "\n|\r"], value=["", ""], regex=True, inplace=True)

        # handle empty strings
        for c in df[required_columns]:
            # if '' in df[c].values:
            if any(df[c] == ''):
                df[c] = df[c].replace({'': ' '})

        # check if tinyint is convertible to boolean
        for c in df[tinyint_columns]:
            try:
                df[c] = df[c].astype('boolean')
            except TypeError as e:
                error_indices_list.extend(
                    df[(df[c] != 1) & (df[c] != 0) & (df[c].notna())].index.values.astype(int).tolist())
                print('Column', c, 'from table', table_name, 'cannot be converted from tinyint to boolean. Please '
                                                             'check the values manually. The corrupt rows are written '
                                                             'in the file corrupt_rows.csv')
                print(e.args[0])

        # check if time is convertible to str
        for c in df[time_columns]:
            try:
                # for i, row in df.iterrows():
                # t = f'{df.loc[i, c].components.hours:02d}:{df.loc[i, c].components.minutes:02d}:{df.loc[i, c].components.seconds:02d}' if not pd.isnull(df.loc[i, c]) else ''
                # df.at[i, c] = t
                df[c] = df[c].apply(
                    lambda x: f'{x.components.hours:02d}:{x.components.minutes:02d}:{x.components.seconds:02d}'
                    if not pd.isnull(x) else ''
                )
            except TypeError as e:
                # todo: handle corrupt rows
                # for i, row in df.iterrows():
                #     try:
                #         df.loc[i, c] = df.loc[i, c].apply(
                #             lambda x: f'{x.components.hours:02d}:{x.components.minutes:02d}:{x.components.seconds:02d}'
                #             if not pd.isnull(x) else ''
                #         )
                #     except TypeError:
                #         error_indices_list.append(i)
                print('Column', c, 'from table', table_name, 'cannot be converted from tinyint to boolean. '
                                                             'Please check the values manually. The corrupt '
                                                             'rows are written in the file corrupt_rows.csv')
        # make sure indices are distinct
        error_indices_set = set(error_indices_list)
        all_indices = df.index.values.astype(int).tolist()
        correct_indices_list = [x for x in all_indices if x not in error_indices_set]
        df_error = df.loc[error_indices_list]
        if not df_error.empty:
            df_error.to_csv('corrupt_rows.csv', index=False)
        df_correct = df.loc[correct_indices_list]
        print('Data from table', table_name, 'cleaned successfully')

    return df_correct


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


def compare_row_count(table_name, database_id, con, date_filter, mysql_schema_name):
    bq_row_count = get_row_count('bigquery', client, database_id, table_name, date_filter)
    mysql_row_count = get_row_count('mysql', con, mysql_schema_name, table_name, date_filter)
    if bq_row_count == mysql_row_count:
        print('Yay destination and source have the same row count for table', table_name, '!')
        return True
    else:
        print('Oops, destination and source have different row counts for table', table_name, '!')
        return False


def start_pipeline(project_id, database_id, table_names, con):
    for table_name in table_names:
        date_filter = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if compare_row_count(table_name, database_id, con, date_filter, 'sparrks'):
            mysql_create_schema_query = get_mysql_table_schema(con, table_name)
            schema_def, required_columns, tinyint_columns, time_columns = prepare_bigquery_schema(
                mysql_create_schema_query,
                table_name)
            table_id = "{}.{}.{}".format(project_id, database_id, table_name)
            create_bigquery_table(table_id, schema_def)
            table_data = get_raw_data_from_mysql(table_name, con)
            table_data = clean_mysql_data(table_name, table_data, required_columns, tinyint_columns, time_columns)
            write_data_to_bigquery(table_data, schema_def, table_id, table_name)

    SQA_CONN_PUB.close()


# def one_table(project_id, database_id, con, table_name ='application_area'):
#     mysql_create_schema_query = get_mysql_table_schema(con, table_name)
#     schema_def, required_columns = prepare_bigquery_schema(mysql_create_schema_query, table_name)
#     table_id = "{}.{}.{}".format(project_id, database_id, table_name)
#     create_bigquery_table(table_id, schema_def)
#     table_data = get_data_from_mysql(table_name, con, required_columns)
#     write_data_to_bigquery(table_data, schema_def, table_id, table_name)


if __name__ == '__main__':
    table_names = get_mysql_table_names(SQA_CONN_PUB)
    start_pipeline(PROJECT_ID, DATABASE_ID, table_names, SQA_CONN_PUB)

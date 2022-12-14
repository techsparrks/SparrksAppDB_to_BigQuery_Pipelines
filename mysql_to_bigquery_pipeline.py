import time
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from data_quality_check import get_row_count
from db_config import SQA_CONN_PUB, DATABASE
from db_queries import get_mysql_table_names_query, get_data_from_mysql_query
from shared_functions import write_data_to_bigquery, create_bigquery_table, get_mysql_table_schema, \
    prepare_bigquery_schema

# BigQuery project and database id
PROJECT_ID = 'data-analytics-359712'
DATABASE_ID = 'sparrksapp_raw_data'

# get BigQuery credentials and connect client
credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-c05ab5a3dc2a.json')
bigquery_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_mysql_table_names(con, database_name):
    """
    Gets the names of all tables in the MySQL database

    con: MySQL connection

    returns: a list of all table names in the MySQL database
    """
    table_names_df = pd.read_sql(get_mysql_table_names_query.format(database_name), con=con)
    table_names = table_names_df['TABLE_NAME'].values.tolist()

    return table_names


def get_raw_data_from_mysql(table_name, database_name, con):
    """
    Gets the raw data from the specified table in MySQL

    table_name: name of the table to get data from
    con: MySQL connection

    returns: a dataframe with data from the specified table
    """
    try:
        df = pd.read_sql(get_data_from_mysql_query.format(database_name, table_name), con=con)
        # print('Raw data from table', table_name, 'fetched successfully')
    except Exception as e:
        df = None
        print(e.args[0])
        print('Fetching raw data from table', table_name, 'failed!')

    return df


def clean_mysql_data(table_name, df, required_columns, tinyint_columns, time_columns):
    """
    Prepares the raw data from MySQL for upload to BigQuery and exports a csv file with corrupt data
    that has to be handled manually

    table_name: name of the table which is to be cleaned
    df: dataframe with the data of the table
    required_columns: list of required columns for the specified table (NOT NULL columns)
    tinyint_columns: list of columns of data type tinyint
    time_columns: list of columns of data type time

    returns: a dataframe in the correct form to be uploaded to BigQuery
    """
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
                print('Column', c, 'from table', table_name, 'cannot be converted from time to string. '
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


def compare_row_count(table_name, database_id, bigquery_client, con, date_filter, mysql_schema_name):
    """
    Compares the row count of a BigQuery table to the row count of the same table in MySQL up to
    a certain point in time (date) to ensure data quality

    table_name: name of the table to be compared
    database_id: ID of the BigQuery database
    bigquery_client: BigQuery client
    con: MySQL connection
    date_filter: date up to which the number of rows of the tables should be compared
    mysql_schema_name: name of the MySQL schema (layer) in which the table is stored in

    returns: True if the row count is the same for the two tables and False otherwise
    """
    bq_row_count = get_row_count(table_name, 'bigquery', bigquery_client, database_id, date_filter)
    mysql_row_count = get_row_count(table_name, 'mysql', con, mysql_schema_name, date_filter)
    if bq_row_count == mysql_row_count:
        print('Data Quality Check passed for table', table_name + '!')
        return True
    else:
        return False


def start_pipeline(table_name, project_id, database_id, bigquery_client, database_name, con, date_filter):
    """
    Starts the MySQL to BigQuery pipeline for a specified table.
    If the table in MySQL and in BigQuery have the same row count up to the specified date (date_filter),
    the pipeline is started, otherwise an error message is shown.
    First the MySQL table creation schema is fetched from MySQL and transformed into a BigQuery schema.
    Then a BigQuery table is created (if not already present).
    The MySQL data is fetched and prepared for a BigQuery import.
    Finally, the cleaned data is uploaded to BigQuery by overwriting the existing data.

    table_name: table for which the pipeline should be executed
    project_id: ID of the BigQuery project
    database_id: ID of the BigQuery database
    bigquery_client: BigQuery client
    con: MySQL connection
    date_filter: date up to which the number of rows of the tables should be compared
    """
    if compare_row_count(table_name, database_id, bigquery_client, con, date_filter, 'sparrks'):
        mysql_create_schema_query = get_mysql_table_schema(con, table_name)
        schema_def, required_columns, tinyint_columns, time_columns = prepare_bigquery_schema(
            mysql_create_schema_query,
            table_name)
        table_id = "{}.{}.{}".format(project_id, database_id, table_name)
        create_bigquery_table(bigquery_client, table_id, schema_def)
        table_data = get_raw_data_from_mysql(table_name, database_name, con)
        table_data = clean_mysql_data(table_name, table_data, required_columns, tinyint_columns, time_columns)
        write_data_to_bigquery(table_data, bigquery_client, schema_def, table_id)
    else:
        print('Data Quality Check failed: unequal row count for table', table_name + '!')


if __name__ == '__main__':
    # set date filter up to which the row count should be compared for data validation
    # date_filter = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_filter = '2022-11-01 10:13:24'

    # get table names for which the pipeline should be executed
    # table_names = get_mysql_table_names(SQA_CONN_PUB, DATABASE)
    table_names = ['coachee_survey', 'coachee_journey_bookings', 'coaches', 'usecases']
    for table_name in table_names:
        start_pipeline(table_name, PROJECT_ID, DATABASE_ID, bigquery_client, DATABASE, SQA_CONN_PUB, date_filter)

    # close MySQL connection
    SQA_CONN_PUB.close()

import re

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from sqlalchemy import text

from db_queries import get_mysql_table_schemas_query

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


def write_data_to_bigquery(df, client, bigquery_schema_def, table_id, overwrite=True):
    """
    Writes data to the table table_id in BigQuery

    df: data to be written to BigQuery in the form of a DataFrame
    client: BigQuery client
    bigquery_schema_def: schema for the table to be created
    table_id: table_id of the BigQuery table to write to

    """

    # set the argument to overwrite the table or to append the new data
    if overwrite:
        overwrite_or_append = 'WRITE_TRUNCATE'
    else:
        overwrite_or_append = 'WRITE_APPEND'

    # write data to BigQuery table
    job_config = bigquery.LoadJobConfig(schema=bigquery_schema_def,
                                        write_disposition=overwrite_or_append,
                                        autodetect=False,
                                        source_format=bigquery.SourceFormat.CSV
                                        )
    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        print('Data for table', table_id, 'uploaded successfully to BigQuery')
    except Exception as e:
        print(e.args[0])
        print('Upload of data for table', table_id, 'to BigQuery failed')


def create_bigquery_table(client, table_id, bigquery_schema_def, recreate_table=False):
    """
    Creates a BigQuery table with table_id using the defined schema

    client: BigQuery client
    table_id: table_id of the BigQuery table to create
    bigquery_schema_def: schema for the table to be created
    recreate_table: specifies if an existing table should be recreated; default value set to False
    """

    try:
        client.get_table(table_id)
        if recreate_table:
            client.delete_table(table_id, not_found_ok=True)
            table = bigquery.Table(table_id, schema=bigquery_schema_def)
            table = client.create_table(table)
            print("Recreated table {}".format(table_id))
        else:
            print("Table {} exists already".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=bigquery_schema_def)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}".format(table_id))


def get_mysql_table_schema(con, table_name):
    """
    Gets the MySQL DDL for table creation

    con: specifies the MySQL connection
    table_name: the name of the table for which the schema should be fetched

    returns: schema for the specified table
    """
    try:
        result = con.execute(text(get_mysql_table_schemas_query.format(table_name)))
        print('Table schema for table', table_name, 'fetched successfully')
    except Exception as e:
        print(e.args[0])
        print('Table schema for table', table_name, 'not fetched.')

    return result.first()[1]


def prepare_bigquery_schema(mysql_schema, table_name):
    """
    Transforms MySQL table creation schema into BigQuery schema

    mysql_schema: the MySQL table schema for table creation
    table_name: the name of the table for which the schema should be transformed

    returns:
        big_query_schema_def: the transformed BigQuery schema definition
        required_columns: list of required columns (NOT NULL columns)
        tinyint_columns: list of columns of data type tinyint
        time_columns: list of columns of data type time
    """
    big_query_schema_def = []
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
                # if 'NOT NULL' in line:
                #     mode_info = 'REQUIRED'
                #     required_columns.append(column_name)
                # else:
                #     mode_info = 'NULLABLE'
                mode_info = 'NULLABLE'
                schema_info = bigquery.SchemaField(column_name, datatype, mode_info)
                big_query_schema_def.append(schema_info)
        print('Schema for table', table_name, 'created')
    except Exception as e:
        print(e.args[0])
        print('Schema creation for table', table_name, 'failed')

    return big_query_schema_def, required_columns, tinyint_columns, time_columns

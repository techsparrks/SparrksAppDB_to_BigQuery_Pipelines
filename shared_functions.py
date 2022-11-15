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


def write_data_to_bigquery(df, client, schema_def, table_id, table_name):
    # post to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema_def,
                                        write_disposition="WRITE_TRUNCATE",  # "WRITE_APPEND"
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


def create_bigquery_table(client, table_id, bigquery_schema_def):
    try:
        client.get_table(table_id)
        print("Table {} exists already".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=bigquery_schema_def)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}".format(table_id))


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
                # if 'NOT NULL' in line:
                #     mode_info = 'REQUIRED'
                #     required_columns.append(column_name)
                # else:
                #     mode_info = 'NULLABLE'
                mode_info = 'NULLABLE'
                schema_info = bigquery.SchemaField(column_name, datatype, mode_info)
                schema_def.append(schema_info)
        print('Schema for table', table_name, 'created')
    except Exception as e:
        print(e.args[0])
        print('Schema creation for table', table_name, 'failed')

    return schema_def, required_columns, tinyint_columns, time_columns

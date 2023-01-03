from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


def get_row_count(table_name, from_str, client_con, dataset, created_at):
    """
    Get the row count for the specified table from the respective database

    table_name: name of the table to get the row count of
    from_str: specifies from which database the table is: bigquery or mysql
    client_con: database connection
    dataset: BigQuery database ID or MySQL schema name
    created_at: date up to which the number of rows of the tables should be compared

    returns: the row count in integer
    """
    sql = """SELECT COUNT(1) as record_count FROM {}.{} where created_at <= '{}'""".format(dataset, table_name, created_at)
    if from_str == 'bigquery' and client_con is not None:
        try:
            job = client_con.query(sql)
            rows = list(job.result())
        except BadRequest as e:
            print(e)
            return 0
        # for row in rows:
        row_count = rows[0].record_count
    elif from_str == 'mysql' and client_con is not None:
        row_count = client_con.execute(sql).scalar()
    else:
        row_count = None
        print('Fetching the row count of table', table_name, 'from', from_str, 'failed.')
    return row_count


# example methods for further Data Quality checks
def getSql(bqDataset, bqTable, columnList, rowCount):
    colIdx = 0
    sql = ""
    prefix = ""
    for col in columnList:
        if colIdx != 0:
            prefix = "UNION ALL" + "\n"

        colName = """'{}'""".format(col)
        minVal = """MIN({})""".format(col)
        maxVal = """MAX({})""".format(col)
        nullVal = """COUNT(CASE WHEN {} IS NULL THEN 1 END)""".format(col)
        uniqVal = """COUNT(DISTINCT({}))""".format(col)
        selVal = """CONCAT(ROUND({}/{} * 100, 2), '%')""".format(uniqVal, rowCount)
        denVal = """CONCAT(ROUND(({} - {})/{} * 100, 2), '%')""".format(rowCount, nullVal, rowCount)

        sql = sql + prefix + """SELECT
                                    {} as ColumnName,
                                    CAST({} as String) as MinValue,
                                    CAST({} as String) as MaxValue,
                                    CAST({} as String) as NullValues,
                                    CAST({} as String) as Cardinality,
                                    CAST({} as String) as Selectivity,
                                    CAST({} as String) as Density
                                FROM {}.{}
                                """.format(colName, minVal, maxVal, nullVal, uniqVal, selVal, denVal, bqDataset, bqTable)
        colIdx = colIdx + 1
    return sql


def runSql(datasetName, dataTable, reportTable, sqlQuery):
    tableRef = """{}.{}""".format(datasetName, dataTable)
    reportRef = """{}.{}""".format(datasetName, reportTable)
    bigqueryClient = bigquery.Client()
    sql = """INSERT INTO {} 
                SELECT current_timestamp, '{}' as TableRef, dq.* 
                FROM ({}) dq""".format(reportRef, tableRef, sqlQuery)

    sqlJob = bigqueryClient.query(sql)
    sqlJob.result()
    return 0

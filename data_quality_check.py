from google.cloud import bigquery

from db_config import SQA_CONN_PUB, SQA_CONN_PUB_ENGINE


def get_row_count(from_str, client_con, dataset, table_name, created_at):
    sql = """SELECT COUNT(1) as record_count FROM {}.{} where created_at <= '{}'""".format(dataset, table_name, created_at)
    if from_str == 'bigquery' and client_con is not None:
        job = client_con.query(sql)
        rows = job.result()
        for row in list(rows):
            row_count = row.record_count
    elif from_str == 'mysql' and client_con is not None:
        row_count = client_con.execute(sql).scalar()
    else:
        row_count = None
        print('Fetching the row count of table', table_name, 'from', from_str, 'failed.')
    return row_count


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

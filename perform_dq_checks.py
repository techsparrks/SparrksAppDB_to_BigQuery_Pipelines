import yaml

from data_quality_check import getRowCount, getSql, runSql

configFile = "./config/dq.yaml"
configList = yaml.safe_load(open(configFile))

for tableConf in configList:
    datasetName = tableConf['datasetName']
    dataTable = tableConf['dataTable']
    reportTable = tableConf['reportTable']
    columnList = tableConf['includeColumns']

    # Get Row Count
    rowCount = getRowCount(datasetName, dataTable)

    # Prepare Dynamic SQL Statement
    sqlQuery = getSql(datasetName, dataTable, columnList, rowCount)

    # Run SQL & Display Output
    runSql(datasetName, dataTable, reportTable, sqlQuery)
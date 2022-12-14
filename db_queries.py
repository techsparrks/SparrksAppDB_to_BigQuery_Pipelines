get_mysql_table_names_query = '''
SELECT table_name
FROM information_schema.tables
WHERE TABLE_TYPE = "BASE TABLE"
AND TABLE_SCHEMA = '{}'
'''

get_mysql_table_schemas_query = '''
SHOW CREATE TABLE {}
'''

get_data_from_mysql_query = '''
select * 
from {}.{} '''
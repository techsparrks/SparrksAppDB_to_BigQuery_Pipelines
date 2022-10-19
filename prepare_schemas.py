import re

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from db_config import SQA_CONN_PUB

sql_statement = '''
CREATE TABLE `coaches` (
  `coach_id` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(1000) NOT NULL,
  `last_name` varchar(1000) NOT NULL,
  `email_id` varchar(1000) NOT NULL,
  `password` varchar(1000) DEFAULT NULL,
  `is_forgot_password_initiated` tinyint(1) DEFAULT NULL,
  `profile_image_path` varchar(2000) DEFAULT NULL,
  `time_zone` varchar(2000) DEFAULT NULL,
  `gender` varchar(1000) NOT NULL,
  `salutation` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `title` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `country_id` int NOT NULL,
  `address` varchar(2000) NOT NULL,
  `website` varchar(2000) DEFAULT NULL,
  `mobile` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `contact_status` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `rate` int DEFAULT NULL,
  `wordpress` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `coach_video_url` varchar(2000) DEFAULT NULL,
  `quality` char(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `details` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `birthday_date` date NOT NULL,
  `german` tinyint(1) NOT NULL,
  `english` tinyint(1) NOT NULL,
  `industry_experience` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `other_industry_experience` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `coaching_session_week` int DEFAULT NULL,
  `years_of_experience` int NOT NULL,
  `experience_coaching` int NOT NULL,
  `experience_in_leading` int NOT NULL,
  `coaching_hours` int DEFAULT NULL,
  `certificates` varchar(2000) DEFAULT NULL,
  `other_certificates` varchar(2000) DEFAULT NULL,
  `company_name` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `tax_id` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `tax_number` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `bank_name` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `owner` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `iban` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `bic` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`coach_id`)
) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=latin1;
'''

con = SQA_CONN_PUB
credentials = service_account.Credentials.from_service_account_file(
    'data-analytics-359712-1f4a4a01f7ba.json')
project_id = 'data-analytics-359712'
client = bigquery.Client(credentials=credentials, project=project_id)

datatypes_dict = {'int': 'INTEGER',
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

schema_def = []
bigquery.SchemaField("coach_id", "STRING", mode="REQUIRED"),

for line in sql_statement.splitlines():
    if re.match(r'^\s*`', line):
        column_name = re.findall(r'(`[^"]*)`', line)[0][1:]
        datatype_info = line.split("`", 2)[2].strip().split(' ')[0].split('(')[0]
        if datatype_info in datatypes_dict.keys():
            datatype = datatypes_dict.get(datatype_info)
        else:
            print(datatype_info)
            datatype = 'STRING'
        if 'NOT NULL' in line:
            mode_info = 'REQUIRED'
            print(column_name)
        else:
            mode_info = 'NULLABLE'
        schema_info = bigquery.SchemaField(column_name, datatype, mode_info)
        schema_def.append(schema_info)

s = schema_def

# table_ref = client.dataset('crm_data').table('coaches')
table_id = "data-analytics-359712.crm_data.coaches"
try:
    client.get_table(table_id)  # Make an API request.
except NotFound:
    table = bigquery.Table(table_id, schema=schema_def)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}".format(table_id)
    )


# get data from MySQL
query = '''
select * 
from sparrks.coaches c '''
df = pd.read_sql(query, con=con)
con.close()
print(df.isna().sum())
# for c in df.columns:
#     print(c, df[c].dtypes)
    # df[c] = df[c].astype('string')

## post to BigQuery
job_config = bigquery.LoadJobConfig(schema=schema_def,
                                    autodetect=False,
                                    source_format=bigquery.SourceFormat.CSV
                                    )

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)
job.result()

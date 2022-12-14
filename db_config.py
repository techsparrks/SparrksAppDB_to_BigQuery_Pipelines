# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

Author: iliyana.tarpova@sparrks.de
"""

from urllib.parse import quote_plus

import sqlalchemy
from load_credentials import load_sparrksapp_db_credentials as load

db_prod_or_test = 'prod_'  # 'test_'

### Credentials
USER = load(db_prod_or_test + 'username')
PASSWORD = load(db_prod_or_test + 'password')
SERVER = load(db_prod_or_test + 'server')
DATABASE = load(db_prod_or_test + 'database')
PORT = load(db_prod_or_test + 'port')
SQA_CONN_STR = f"mysql+pymysql://{USER}:%s@{SERVER}:{PORT}/{DATABASE}" % quote_plus(PASSWORD)
SQA_CONN_PUB_ENGINE = sqlalchemy.create_engine(SQA_CONN_STR)
SQA_CONN_PUB = SQA_CONN_PUB_ENGINE.connect()

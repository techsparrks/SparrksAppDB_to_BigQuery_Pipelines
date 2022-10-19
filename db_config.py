# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

Author: iliyana.tarpova@sparrks.de
"""





import sqlalchemy
from load_credentials import load_sparrksapp_db_credentials as load


### Credentials
USER = load('username')
PASSWORD = load('password')
SERVER = load('server')
DATABASE = load('database')
PORT = load('port')
SQA_CONN_STR = f"mysql+pymysql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
SQA_CONN_PUB_ENGINE = sqlalchemy.create_engine(SQA_CONN_STR)
SQA_CONN_PUB = SQA_CONN_PUB_ENGINE.connect()



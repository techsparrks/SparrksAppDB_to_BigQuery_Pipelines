# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

Author: Mojtaba.peyrovi@sparrks.de
"""


import os
import configparser
    


def load_sparrksapp_db_credentials(label):
    """
    Read the database credentials from the config file.
    
    """

    config = configparser.ConfigParser()
    root = os.getcwd()
    config.read(str(root) + '/config.ini')
    cfg = config['staging credentials']
    return cfg[label]
	







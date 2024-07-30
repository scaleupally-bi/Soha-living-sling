import pyodbc
from pytz import timezone
from datetime import datetime
import sqlalchemy
import os
from pathlib import Path
import json
from utility.connector import *
from utility.request import Api


root_dir = Path(__file__).parent.parent

CURRENT_DATETIME = datetime.now(timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
CURRENT_TIME  = datetime.now(timezone('Asia/Kolkata')).strftime('%H:%M:%S')


# setup the environment variable from json file
# -----------------------------------------------------------
'''
for example: 
if os.path.exists(str(root_dir)+'/salesforce/credentials.json'):
  
    If env.json exists, load the environment variables from it.

    Example `env.json`:
    {
        "DB_USER": "user",
        "DB_NAME": "Database name",
        "DB_PASSWORD": "password",
        "DB_HOST": "localhost",
        "DB_PORT": "1433"
    }
 
   
    with open(str(root_dir)+'/salesforce/credentials.json') as f:
        env = json.load(f)
        os.environ.update(env)
'''

# -------------------------------------------------------------

if os.path.exists(str(root_dir)+'/utility/constants/db_credentials.json'):

 
   
    with open(str(root_dir)+'/utility/constants/db_credentials.json') as f:
        env = json.load(f)
        os.environ.update(env)

if os.path.exists(str(root_dir)+'/utility/constants/soha-living.json'):

 
   
    with open(str(root_dir)+'/utility/constants/soha-living.json') as f:
        env = json.load(f)
        os.environ.update(env)


def Session():
    # Creating an object of the Destination DataMart 
    server = Server()
    return server.connectors()
    # return server.sql_server_engine()


def Apis():
    # Creating an api token for api requests
    api=Api()
    return api
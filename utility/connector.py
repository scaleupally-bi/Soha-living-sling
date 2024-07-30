from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker 
from utility.constants import *
from urllib.parse import quote_plus
import os

import sqlalchemy

import urllib.parse

password = "Sql%@1234"
encoded_password = urllib.parse.quote(password)

"""
Server class creates an engine to the destination data source.

The connection requires pyodbc and odbc connection for mssql
"""
class Server():
    def __init__(self) -> None:
        pass

    '''
        Creating a DataBase Engine for Data Insertion
    '''

    
    def connectors(self):

        encoded_password = urllib.parse.quote(os.getenv("PWD"))

        connection_string = 'mssql+pyodbc://{username}:{password}@{server_name}/{db_name}?driver={driver_name}'.format(
            username=os.getenv("UID"),
            password=encoded_password,
            server_name=os.getenv("server_name"),
            db_name=os.getenv("db_name"),
            driver_name=os.getenv("driver_name")
        )
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    # def sql_server_engine(self):

    #     connection_string = 'mssql+pyodbc://{server_name}/{db_name}?driver={driver_name}'.format(
    #         server_name="DESKTOP-B5JF1E0",
    #         db_name="SohaLiving",
    #         driver_name="ODBC Driver 17 for SQL Server"
    #     )
    #     print('os.getenv("server_name"):',os.getenv("server_name"))
    #     DATABASE_ENGINE = sqlalchemy.create_engine(connection_string)
    #     Session = sessionmaker(bind=DATABASE_ENGINE)
    #     session = Session()
    #     print("session:", session)
    #     return session

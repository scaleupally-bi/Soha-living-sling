from sqlalchemy import inspect
from .api import *
from sqlalchemy.orm import class_mapper
from models import *
import pandas as pd
from sqlalchemy.orm import Session
import numpy as np
import math
from datetime import timezone
from .connector import *
from utility.setting import *
from sqlalchemy.sql import text
from datetime import datetime, timedelta

session=Session()


def make_object_for_creation_values_to_database(model,column_key_and_value:dict):
    '''
    We can assign all the field value using this function.

    Argument:
    - model : Data model
    - column_key_and_value : In this argument pass the dictionary also matching the keys with model fields name.
    
    '''
    model.__dict__.update(column_key_and_value)
    return model


def get_column_names(model):
    return [column.key for column in class_mapper(model).columns]

def get_datetime_column(table_name):
    '''
    We can get the all data datatype columns in the list.

    Argument:
    - table_name : The pass table which you have to get table column.
    
    '''
    # Create an inspector and get column names with datetime and date values
    inspector = inspect(session)
    datetime_columns = [column['name'] for column in inspector.get_columns(f'{table_name}') if isinstance(column['type'], (DateTime)) and column['name'] !='SystemUpdateAt' and column['name']!='SystemCreatedAt']
    return datetime_columns



def bulk_create(model, df: pd.DataFrame, temp_table_query,file_name,table_name):
    '''
    We will bulk create and update rows using this function.

    Arguments:
    - model : this is temporary data model.
    - df : This is data frame, accept, all sorted and processed data.
    - temp_table_query : temp_table_query contains the query to create a temporary table to store all data coming from API.
    - procedure_name : procedure_name is the name of procedure that insert or updates the records in main table.
    '''


    df.replace("'", "`", regex=True, inplace=True) # replace ' to ` for example don't to don`t, why we use this?, because the data don't save with ' this.
    df = df.replace({np.nan: None})
    df = df.to_dict("records") # df to dict records.
    
    df_length = len(df)

    old_records_query = f"SELECT COUNT(*) FROM {table_name};"
    old_records_result = session.execute(text(old_records_query))
    old_records = old_records_result.scalar()

    session.execute(text(temp_table_query))
    session.bulk_insert_mappings(model,df)      
    session.execute(text(f"{getStoreProcedure(file_name)}"))
    session.commit()

    new_records_result = session.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
    new_records = new_records_result.scalar()

    created_records = new_records - old_records
    if created_records <= 0:
        created_records = 0
        updated_records = df_length
    else:
        updated_records = df_length - created_records

    session.close()
    records = {
        "total_no_of_insert":abs(created_records),
        "total_no_of_update":abs(updated_records)
    }
    return records

def getStoreProcedure(filename):
    file_content = ''
    if os.path.exists(str(root_dir)+f'/StoredProcedure/{filename}'):
        with open(str(root_dir)+f'/StoredProcedure/{filename}','r',encoding='utf-8') as f:
            file_content+=f.read()
    return file_content



def saveLogSheet(table_name,status,exception,start_time,end_time,total_no_of_insert,total_no_of_update):
    created_date = datetime.now(timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    data_dict = {
        "table_name":[table_name],
        "total_no_of_insert_record":[total_no_of_insert],
        "total_no_of_update_record":[total_no_of_update],
        "start_time":[start_time],
        "end_time":[end_time],
        "status":[status],
        "created_at":[created_date]
    }
    if exception:
        data_dict['exception'] = f"'{exception}'"

    df = pd.DataFrame(data_dict)
    df.replace("'", "`", regex=True, inplace=True) # replace ' to ` for example don't to don`t, why we use this?, because the data don't save with ' this.
    data_dict = df.to_dict("records") # df to dict records.
    # object = 

    # sesseion.add(make_object_for_creation_values_to_database(LogSheet(),data_dict[0])) write a code for logs

    session.commit()
 
    logs = f"""
    //==================================================
    // Table '{table_name}' : Logged At {created_date}
    //==================================================
    //--------------------------------------------------
    // Service Status : {status}
    // Exception      : {exception}
    //--------------------------------------------------

    - Table Name    : {table_name}
    - Start Time    : {start_time}
    - End Time      : {end_time}
    - No Of Insert Records : {total_no_of_insert}
    - No Of Update Records : {total_no_of_update}

    """
    print(logs)
    return True


def convert_boolean_fields_to_lower(df,field_list):
    for field in field_list:
        df[field] = df[field].astype(str)
        df[field] = df[field].str.lower()

    return df


def get_boards_ids(model):
    results = session.query(model).filter().all()

    # Extracting ids from results
    ids_found = [result.id for result in results]

    return ids_found

def get_datetime_columns(model):
    # Get the columns from the model using SQLAlchemy's inspection system
    columns = inspect(model).columns
    # Extract column names with DateTime data type
    datetime_columns = [column.name for column in columns if isinstance(column.type, DateTime)]
    return datetime_columns

def get_date_columns(model):
    # Get the columns from the model using SQLAlchemy's inspection system
    columns = inspect(model).columns
    # Extract column names with DateTime data type
    date_columns = [column.name for column in columns if isinstance(column.type, Date)]
    return date_columns

def convert_str_to_datetime(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df

def convert_str_to_date(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
    return df


def convert_country_code(df,column):
    df[column] = df[column].astype(str)

    for i in range(len(df)):
        if df.loc[i, column] != 'None':
            df.loc[i, column] = f'+{str(int(df.loc[i, column]))}'

    return df



def get_float_columns(model):
    # Get the columns from the model using SQLAlchemy's inspection system
    columns = inspect(model).columns
    # Extract column names with DateTime data type
    float_columns = [column.name for column in columns if isinstance(column.type, Float)]
    return float_columns


def round_float_columns(df,columns_names):
    for col in columns_names:
        df[col] = df[col].astype(float)
        df[col] = df[col].round(2)
    return df


def save_logs(table_name,start_time,end_time,error,number_of_records_created,number_of_records_updated,created_at):

    obj = Logsheet(
        tableName=table_name,
        startTime=start_time,
        endTime=end_time,
        error = error,
        noRecordsCreated=number_of_records_created,
        noRecordsUpdated=number_of_records_updated,
        createdAt=datetime.now()
    )
    session.add(obj)
    session.commit()
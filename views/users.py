
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  get_column_names,bulk_create, get_datetime_columns, get_date_columns,convert_str_to_datetime,convert_str_to_date,convert_country_code
from utility.temporary_table_query import users_temporary_table_query
import numpy as np
from datetime import datetime
import pandas as pd
from utility.request import *

class Users(Api):
    def extract_users(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # try:
        end_point = 'v1/users/concise'  
        params = {}
        
        response= self.request(end_point,params)  
        if response.status_code==200:
            users=response.json()['users']
            
        else:
            raise Exception(response.content)
                
        df = pd.DataFrame(users)

        column_list = get_column_names(UserTemp)
        df = df[df.columns.intersection(column_list)]

        datetime_columns = get_datetime_columns(UserTemp)
        df = convert_str_to_datetime(df, datetime_columns)

        date_columns = get_date_columns(UserTemp)
        df = convert_str_to_date(df,date_columns)


        df['countryCode'] = df['countryCode'].apply(lambda x: str(x) if x is not None else x)
        df['emergencyContactCountryCode'] = df['emergencyContactCountryCode'].apply(lambda x: str(x) if x is not None else x)

        # convert_country_code(df,'countryCode')
        
        # print(df['emergencyContactCountryCode'])

        file_name = "upsert_users.sql"
        table_name = 'users'
        records = bulk_create(UserTemp,df,users_temporary_table_query,file_name,table_name)
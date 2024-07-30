
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import get_column_names,bulk_create, get_datetime_columns, convert_str_to_datetime
from utility.temporary_table_query import leave_types_temporary_table_query

from datetime import datetime
import pandas as pd
from utility.request import *

class LeaveTypesClass(Api):
    def extract_leave_types(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # try:
        end_point = 'v1/leave/types'  
        params = {}
        response= self.request(end_point,params)  
        if response.status_code==200:
            leave_types = response.json()
        else:
            raise Exception(response.content)
                
        df = pd.DataFrame(leave_types)

        column_list = get_column_names(LeaveTypeTemp)
        df = df[df.columns.intersection(column_list)]

        datetime_columns = get_datetime_columns(LeaveTypeTemp)
        df = convert_str_to_datetime(df,datetime_columns)

        file_name = "upsert_leave_types.sql"
        table_name = 'leave_types'
        records = bulk_create(LeaveTypeTemp,df,leave_types_temporary_table_query,file_name,table_name)
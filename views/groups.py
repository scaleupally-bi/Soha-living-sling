
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import get_column_names,bulk_create, get_datetime_columns, convert_str_to_datetime
from utility.temporary_table_query import groups_temporary_table_query

from datetime import datetime
import pandas as pd
from utility.request import *

session=Session()


class GroupsClass(Api):
    def extract_groups(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/groups'  
            params = {}
            response= self.request(end_point,params)  
            if response.status_code==200:
                groups = response.json()
            else:
                raise Exception(response.content)
                    
            df = pd.DataFrame(groups)

            column_list = get_column_names(GroupTemp)
            df = df[df.columns.intersection(column_list)]

            datetime_columns = get_datetime_columns(GroupTemp)
            df = convert_str_to_datetime(df,datetime_columns)

            file_name = "upsert_groups.sql"
            table_name = 'groups'
            records = bulk_create(GroupTemp,df,groups_temporary_table_query,file_name,table_name)
        except Exception as e:
            print(e)
            session.rollback()
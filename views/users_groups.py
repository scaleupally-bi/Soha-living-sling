
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import get_column_names,bulk_create, save_logs
from utility.temporary_table_query import users_groups_temporary_table_query
from datetime import datetime
import pandas as pd
from utility.request import *

session=Session()


class UsersGroups(Api):
    def extract_users_groups(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/users/concise'  
            params = {}
            group_list = []  
            iter = True
            response= self.request(end_point,params)  
            if response.status_code==200:

                users=response.json()['users']
                for user in users:
                    rows = [{"userId": user['id'], "groupId": group_id} for group_id in user['groupIds']]
                    group_list.extend(rows)

            else:
                raise Exception(response.content)
                    
            df = pd.DataFrame(group_list)

            column_list = get_column_names(UserGroupTemp)
            df = df[df.columns.intersection(column_list)]

            file_name = "upsert_users_groups.sql"
            table_name = 'user_groups'
            records = bulk_create(UserGroupTemp,df,users_groups_temporary_table_query,file_name,table_name)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("user_groups",start_time,end_time,None,records['total_no_of_insert'],records['total_no_of_update'],datetime.now())
        except Exception as e:
            print(e)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("user_groups",start_time,end_time,e,0,0,datetime.now())
            session.rollback()

import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  get_column_names,bulk_create, get_datetime_columns, get_date_columns,convert_str_to_datetime,convert_str_to_date
from utility.temporary_table_query import leave_report_temporary_table_query

from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class LeaveReportClass(Api):
    def extract_leave_report(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/reports/leave'  
            end_date = datetime.today().strftime("%Y-%m-%d")
            end_date = datetime.strptime(end_date,"%Y-%m-%d")
            start_date = end_date - timedelta(days=45)

            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            
            params = {
                "dates":f'{start_date}/{end_date}'
            }
            
            leave_list = []
            response= self.request(end_point,params)  
            if response.status_code==200:
                leave_data=response.json()
                for user_id, records in leave_data.items():
                    remaining_value = records.get("remaining")
                    for key, record in records.items():
                        if key != "remaining":
                            row = {
                                "userId": user_id,
                                "leaveTypeId": key,
                                "approved": record.get("approved"),
                                "approvedMinutes": record.get("approvedMinutes"),
                                "unpaid": record.get("unpaid"),
                                "unpaidMinutes": record.get("unpaidMinutes"),
                                "deniedMinutes":record.get("deniedMinutes",None),
                                "deniedDays":record.get("denied"),
                                "pending": record.get("pending"),
                                "pendingMinutes": record.get("pendingMinutes"),
                                "ptoCost":record.get("ptoCost"),
                                "remaining": remaining_value
                            }
                            leave_list.append(row)
            
            else:
                raise Exception(response.content)
            
                    
            df = pd.DataFrame(leave_list)

            column_list = get_column_names(LeaveReportTemp)
            df = df[df.columns.intersection(column_list)]

            int_columns = df.select_dtypes(include=['int64','float64']).columns
            df[int_columns] = df[int_columns].fillna(0)

            df['remaining'] = df['remaining'].round(2)

            file_name = "upsert_leave_report.sql"
            table_name = 'leave_report'
            records = bulk_create(LeaveReportTemp,df,leave_report_temporary_table_query,file_name,table_name)

        except Exception as e:
            print(e)
            session.rollback()

import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  get_column_names,bulk_create, get_datetime_columns, get_date_columns,convert_str_to_datetime,convert_str_to_date,save_logs
from utility.temporary_table_query import leave_report_temporary_table_query
import time
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
            start_date = end_date - timedelta(days=60)

            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            start_date = "2023-01-03"
            end_date = "2025-04-20"
            
            date_range = pd.date_range(start=start_date, end=end_date)
            date_list = date_range.strftime("%Y-%m-%d").to_list()
            leave_list = []

            for date in date_list:
                time.sleep(1)
                params = {
                    "dates":f'{date}/{date}'
                }
                print("date:",date)
                response= self.request(end_point,params)  
                if response.status_code==200:
                    leave_data=response.json()
                    for user_id, records in leave_data.items():
                        remaining_value = records.get("remaining")
                        for key, record in records.items():
                            if key != "remaining":
                                row = {
                                    "userId": user_id,
                                    "date":date,
                                    "leaveTypeId": key,
                                    "approved": record.get("approved",None),
                                    "approvedMinutes": record.get("approvedMinutes",None),
                                    "unpaid": record.get("unpaid",None),
                                    "unpaidMinutes": record.get("unpaidMinutes",None),
                                    "deniedMinutes":record.get("deniedMinutes",None),
                                    "deniedDays":record.get("denied",None),
                                    "pending": record.get("pending",None),
                                    "pendingMinutes": record.get("pendingMinutes",None),
                                    "ptoCost":record.get("ptoCost",None),
                                    "remaining": remaining_value
                                }
                                leave_list.append(row)
                
                else:
                    raise Exception(response.content)
            
                    
            df = pd.DataFrame(leave_list)

            column_list = get_column_names(LeaveReportTemp)
            df = df[df.columns.intersection(column_list)]

            df['deniedMinutes'] = df['deniedMinutes'].astype(float)
            df['deniedDays'] = df['deniedDays'].astype(float)


            int_columns = df.select_dtypes(include=['int64','float64']).columns
            df[int_columns] = df[int_columns].fillna(0)

            df['remaining'] = df['remaining'].round(2)

            file_name = "upsert_leave_report.sql"
            table_name = 'leave_report'
            records = bulk_create(LeaveReportTemp,df,leave_report_temporary_table_query,file_name,table_name)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("leave_report",start_time,end_time,None,records['total_no_of_insert'],records['total_no_of_update'],datetime.now())
        except Exception as e:
            print(e)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("leave_report",start_time,end_time,e,0,0,datetime.now())
            session.rollback()

    def extract_leave_report_date_wise(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/reports/leave'  
            
            current_date = datetime.today()
            no_last_days = os.getenv("no_last_days")
            no_future_days = os.getenv("no_future_days")
            end_date = current_date + timedelta(days=int(no_future_days))
            start_date = current_date - timedelta(days=int(no_last_days))
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            # start_date = "2023-01-02"
            # end_date = "2025-07-23"

            date_range = pd.date_range(start=start_date, end=end_date)
            date_list = date_range.strftime("%Y-%m-%d").to_list()
            leave_list = []

            for date in date_list:
                print("date:", date)
                time.sleep(1)
                params = {
                    "dates":f'{date}/{date}'
                }
                
                response= self.request(end_point,params)  
                if response.status_code==200:
                    leave_data=response.json()
                    for user_id, records in leave_data.items():

                        remaining_value = records.get("remaining")
                        for key, record in records.items():
                            if key != "remaining":
                                row = {
                                    "date":date,
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

            df['deniedMinutes'] = df['deniedMinutes'].astype(float)
            df['deniedDays'] = df['deniedDays'].astype(float)


            int_columns = df.select_dtypes(include=['int64','float64']).columns
            df[int_columns] = df[int_columns].fillna(0)

            df['remaining'] = df['remaining'].round(2)

            file_name = "upsert_leave_report.sql"
            table_name = 'leave_report'
            records = bulk_create(LeaveReportTemp,df,leave_report_temporary_table_query,file_name,table_name)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("leave_report",start_time,end_time,None,records['total_no_of_insert'],records['total_no_of_update'],datetime.now())

        except Exception as e:
            print(e)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("leave_report",start_time,end_time,e,0,0,datetime.now())
            session.rollback()


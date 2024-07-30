
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  (
    get_column_names,
    bulk_create, 
    get_datetime_columns, 
    get_date_columns,
    convert_str_to_datetime,
    convert_str_to_date,
    get_float_columns,
    round_float_columns
)
from utility.temporary_table_query import payroll_report_temporary_table_query

from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class PayrollReportClass(Api):
    def extract_payroll_report(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # try:
        end_point = 'v1/reports/payroll'  
        end_date = datetime.today().strftime("%Y-%m-%d")
        end_date = datetime.strptime(end_date,"%Y-%m-%d")
        start_date = end_date - timedelta(days=10)

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        start_date = "2023-07-23"
        end_date = "2023-07-30"

        
        params = {
            "dates":f'{start_date}/{end_date}'
        }
        
        response= self.request(end_point,params)  
        if response.status_code==200:
            report_data=response.json()
        
        else:
            raise Exception(response.content)
        
                
        df = pd.DataFrame(pd.json_normalize(report_data))

        rename_columns = {
            "user.id":"userId",
            "location.id":"locationId",
            "position.id":"positionId",
            "shift.id":"shiftId",
            "shift.duration":"shiftDuration",
            "shift.actualDuration":"shiftActualDuration",
            "shift.breakDuration":"shiftBreakDuration",
            "shift.actualBreakDuration":"shiftActualBreakDuration"
        }
        df.rename(columns=rename_columns,inplace=True)

        int_columns = df.select_dtypes(include=['int64','float64']).columns
        int_columns = list(int_columns)
        int_columns.remove("locationId")
        int_columns.remove("positionId")
        df[int_columns] = df[int_columns].fillna(0)

        df.drop_duplicates(subset=['userId','date','locationId','positionId'],inplace=True)

        column_list = get_column_names(PayrollReportTemp)
        df = df[df.columns.intersection(column_list)]

        datetime_columns = get_datetime_columns(PayrollReportTemp)
        df = convert_str_to_datetime(df, datetime_columns)

        date_columns = get_date_columns(PayrollReportTemp)
        df = convert_str_to_date(df,date_columns)

        float_columns = get_float_columns(PayrollReportTemp)
        round_float_columns(df,float_columns)    

        delete = session.query(PayrollReport).filter(PayrollReport.date.between(start_date, end_date)).delete(synchronize_session=False)
        session.commit()

        file_name = "upsert_payroll_report.sql"
        table_name = 'payroll_report'
        records = bulk_create(PayrollReportTemp,df,payroll_report_temporary_table_query,file_name,table_name)


    # def extract_payroll_report(self):
    #     start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     # try:
    #     end_point = 'v1/reports/payroll'  
    #     start_date = '2023-01-01'
    #     end_date = '2023-02-01'
    #     future_date = '2025-08-30'
    #     report_data_list = []
    #     while datetime.strptime(start_date,"%Y-%m-%d")<= datetime.strptime(future_date,"%Y-%m-%d"):
    #         print("start_date:",start_date)
    #         print("end_date:",end_date)
    #         params = {
    #             "dates":f'{start_date}/{end_date}'
    #         }
            
    #         response= self.request(end_point,params)  
    #         if response.status_code==200:
    #             report_data=response.json()
    #             report_data_list.extend(report_data)
                
    #         else:
    #             raise Exception(response.content)
    #         start_date = end_date
    #         end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=90)
    #         end_date = datetime.strftime(end_date,"%Y-%m-%d")
                
    #     df = pd.DataFrame(pd.json_normalize(report_data_list))

    #     rename_columns = {
    #         "user.id":"userId",
    #         "location.id":"locationId",
    #         "position.id":"positionId",
    #         "shift.id":"shiftId",
    #         "shift.duration":"shiftDuration",
    #         "shift.actualDuration":"shiftActualDuration",
    #         "shift.breakDuration":"shiftBreakDuration",
    #         "shift.actualBreakDuration":"shiftActualBreakDuration"
    #     }
    #     df.rename(columns=rename_columns,inplace=True)

    #     int_columns = df.select_dtypes(include=['int64','float64']).columns
    #     int_columns = list(int_columns)
    #     int_columns.remove("locationId")
    #     int_columns.remove("positionId")
    #     df[int_columns] = df[int_columns].fillna(0)

    #     df.drop_duplicates(subset=['userId','date','locationId','positionId'],inplace=True)

    #     column_list = get_column_names(PayrollReportTemp)
    #     df = df[df.columns.intersection(column_list)]

    #     datetime_columns = get_datetime_columns(PayrollReportTemp)
    #     df = convert_str_to_datetime(df, datetime_columns)

    #     date_columns = get_date_columns(PayrollReportTemp)
    #     df = convert_str_to_date(df,date_columns)

    #     float_columns = get_float_columns(PayrollReportTemp)
    #     round_float_columns(df,float_columns)    

    #     file_name = "upsert_payroll_report.sql"
    #     table_name = 'payroll_report'
    #     records = bulk_create(PayrollReportTemp,df,payroll_report_temporary_table_query,file_name,table_name)
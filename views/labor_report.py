
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
    round_float_columns,
    save_logs
)
from utility.temporary_table_query import labor_report_temporary_table_query

from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class LaborReportClass(Api):
    def extract_labor_report_daily(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/reports/labor'  
            current_year = datetime.now().year

            january_first = datetime(current_year, 1, 1)
            start_date = january_first.strftime('%Y-%m-%d')

            january_second = datetime(current_year, 2, 1)
            end_date = january_second.strftime('%Y-%m-%d')

            iter = True
            labor_report_list = []
            while iter == True:
                print("start_date:",start_date)
                print("end_date:",end_date)

                params = {
                    "dates":f'{start_date}/{end_date}'
                }
                
                response= self.request(end_point,params)  
                if response.status_code==200:
                    report_data=response.json()['cost']
                    if len(report_data)>0:
                        labor_report_list.extend(report_data)
                        iter = True
                    else:
                        iter = False
                else:
                    iter = False
                    raise Exception(response.content)
                
                start_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) 
                start_date = start_date.strftime("%Y-%m-%d")
                end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=90)
                end_date = datetime.strftime(end_date,"%Y-%m-%d")
            
            
                    
            df = pd.DataFrame(pd.json_normalize(labor_report_list))

            rename_columns = {
                "user.id":"userId",
                "location.id":"locationId",
                "position.id":"positionId"
            }
            df.rename(columns=rename_columns,inplace=True)

            column_list = get_column_names(LaborReportTemp)
            df = df[df.columns.intersection(column_list)]

            datetime_columns = get_datetime_columns(LaborReportTemp)
            df = convert_str_to_datetime(df, datetime_columns)

            date_columns = get_date_columns(LaborReportTemp)
            df = convert_str_to_date(df,date_columns)

            float_columns = get_float_columns(LaborReportTemp)
            round_float_columns(df,float_columns) 

            df['date'] = pd.to_datetime(df['date'])  
            start_date = df['date'].min()
            end_date = df['date'].max()

            session.query(LaborReport).filter(LaborReport.date.between(start_date, end_date)).delete(synchronize_session=False)
            session.commit()

            file_name = "upsert_labor_report.sql"
            table_name = 'labor_report'
            records = bulk_create(LaborReportTemp,df,labor_report_temporary_table_query,file_name,table_name)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("labor_report",start_time,end_time,None,records['total_no_of_insert'],records['total_no_of_update'],datetime.now())
        except Exception as e:
            print(e)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("labor_report",start_time,end_time,e,0,0,datetime.now())
            session.rollback()

    def extract_labor_report_one_time(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # try:
        end_point = 'v1/reports/labor'  
        start_date = '2023-01-01'
        end_date = '2023-02-01'
        future_date = '2025-08-30'
        report_data_list = []
        while datetime.strptime(start_date,"%Y-%m-%d")<= datetime.strptime(future_date,"%Y-%m-%d"):
            print("start_date:",start_date)
            print("end_date:",end_date)
            params = {
                "dates":f'{start_date}/{end_date}'
            }
            
            response= self.request(end_point,params)  
            if response.status_code==200:
                report_data=response.json()['cost']
                report_data_list.extend(report_data)
                
            else:
                raise Exception(response.content)
            start_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) 
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=90)
            end_date = datetime.strftime(end_date,"%Y-%m-%d")
                
        df = pd.DataFrame(pd.json_normalize(report_data_list))

        rename_columns = {
            "user.id":"userId",
            "location.id":"locationId",
            "position.id":"positionId"
        }
        df.rename(columns=rename_columns,inplace=True)


        column_list = get_column_names(LaborReportTemp)
        df = df[df.columns.intersection(column_list)]

        datetime_columns = get_datetime_columns(LaborReportTemp)
        df = convert_str_to_datetime(df, datetime_columns)

        date_columns = get_date_columns(LaborReportTemp)
        df = convert_str_to_date(df,date_columns)

        float_columns = get_float_columns(LaborReportTemp)
        round_float_columns(df,float_columns)    


        file_name = "upsert_labor_report.sql"
        table_name = 'labor_report'
        records = bulk_create(LaborReportTemp,df,labor_report_temporary_table_query,file_name,table_name)
		
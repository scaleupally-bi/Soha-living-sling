
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
from utility.temporary_table_query import labor_report_temporary_table_query

from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class LaborReportClass(Api):
    # def extract_labor_report(self):
        # start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # # try:
        # end_point = 'v1/reports/labor'  
        # end_date = datetime.today().strftime("%Y-%m-%d")
        # end_date = datetime.strptime(end_date,"%Y-%m-%d")
        # start_date = end_date - timedelta(days=45)

        # start_date = start_date.strftime("%Y-%m-%d")
        # end_date = end_date.strftime("%Y-%m-%d")

        # start_date = "2024-04-01"
        # end_date = "2024-07-31"

        
        # params = {
        #     "dates":f'{start_date}/{end_date}'
        # }
        
        # response= self.request(end_point,params)  
        # if response.status_code==200:
        #     report_data=response.json()['cost']
        
        # else:
        #     raise Exception(response.content)
        
                
        # df = pd.DataFrame(pd.json_normalize(report_data))

        # rename_columns = {
        #     "user.id":"userId",
        #     "location.id":"locationId",
        #     "position.id":"positionId"
        # }
        # df.rename(columns=rename_columns,inplace=True)

        # df.drop_duplicates(subset=['userId','date','locationId','positionId'],inplace=True)

        # column_list = get_column_names(LaborReportTemp)
        # df = df[df.columns.intersection(column_list)]

        # datetime_columns = get_datetime_columns(LaborReportTemp)
        # df = convert_str_to_datetime(df, datetime_columns)

        # date_columns = get_date_columns(LaborReportTemp)
        # df = convert_str_to_date(df,date_columns)

        # df['actualHours'] = df['actualHours'].round(2)
        # df['actualHoursStr'] = df['actualHoursStr'].astype(float)
        # df['actualHoursStr'] = df['actualHoursStr'].round(2)

        # session.query(LaborReport).filter(LaborReport.date.between(start_date, end_date)).delete(synchronize_session=False)
        # session.commit()

        # file_name = "upsert_labor_report.sql"
        # table_name = 'labor_report'
        # records = bulk_create(LaborReportTemp,df,labor_report_temporary_table_query,file_name,table_name)

    def extract_labor_report(self):
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
            start_date = end_date
            end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=90)
            end_date = datetime.strftime(end_date,"%Y-%m-%d")
                
        df = pd.DataFrame(pd.json_normalize(report_data_list))

        rename_columns = {
            "user.id":"userId",
            "location.id":"locationId",
            "position.id":"positionId"
        }
        df.rename(columns=rename_columns,inplace=True)

        df.drop_duplicates(subset=['userId','date','locationId','positionId'],inplace=True)

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
		
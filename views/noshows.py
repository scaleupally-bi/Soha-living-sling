
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  get_column_names,bulk_create, save_logs
from utility.temporary_table_query import noshows_temporary_table_query
from time import sleep
from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class NoshowsClass(Api):
    def extract_noshows(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            end_point = 'v1/reports/noshows'  
            end_date = datetime.today().strftime("%Y-%m-%d")
            end_date = datetime.strptime(end_date,"%Y-%m-%d")
            no_last_days = os.getenv("no_last_days")
            start_date = end_date - timedelta(days=int(no_last_days))

            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            # start_date = "2025-08-25"
            # end_date = "2025-08-30"

            date_range = pd.date_range(start=start_date, end=end_date)
            date_list = date_range.strftime("%Y-%m-%d").to_list()
            noshows_list = []
            for date in date_list:
                sleep(1)
                print("date:",date)
                params = {
                    "dates":f'{date}/{date}'
                }
                
                response= self.request(end_point,params)  
                if response.status_code==200:
                    report_data=response.json()
                    for i in report_data.items():
                        
                        groups = i[0].split("/")
                        noshow_dict = i[1]
                        for grp in range(len(groups)):
                            noshow_dict[f"group{grp}"] = groups[grp]
                        noshow_dict["date"] = date
                        noshows_list.append(noshow_dict)
            
                else:
                    raise Exception(response.content)
            
                    
            df = pd.DataFrame(noshows_list)
            rename_columns = {
                "group0":"userId",
                "group1":"locationId",
                "group2":"positionId",
            }
            df.rename(columns=rename_columns,inplace=True)
            int_columns = df.select_dtypes(include=['int64','float64']).columns
            df[int_columns] = df[int_columns].fillna(0)

            df.drop_duplicates(subset=['userId','locationId','positionId','date'],inplace=True)

            column_list = get_column_names(NoShowsTemp)
            df = df[df.columns.intersection(column_list)]

            file_name = "upsert_noshows.sql"
            table_name = 'noshows'
            records = bulk_create(NoShowsTemp,df,noshows_temporary_table_query,file_name,table_name)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("noshows",start_time,end_time,None,records['total_no_of_insert'],records['total_no_of_update'],datetime.now())
        except Exception as e:
            print(e)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_logs("noshows",start_time,end_time,e,0,0,datetime.now())
            session.rollback()
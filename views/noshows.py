
import datetime
from io import StringIO
from sqlalchemy import func

from utility.constants import *
from utility.constants import *
from utility.setting import *
from utility.request import *
from models.models import *
from utility.helper import  get_column_names,bulk_create
from utility.temporary_table_query import noshows_temporary_table_query

from datetime import datetime,timedelta
import pandas as pd
from utility.request import *

session=Session()


class NoshowsClass(Api):
    def extract_noshows(self):
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # try:
        end_point = 'v1/reports/noshows'  
        end_date = datetime.today().strftime("%Y-%m-%d")
        end_date = datetime.strptime(end_date,"%Y-%m-%d")
        start_date = end_date - timedelta(days=45)

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        start_date = '2022-01-01'
        end_date = '2025-08-25'

        
        params = {
            "dates":f'{start_date}/{end_date}'
        }
        
        noshows_list = []

        response= self.request(end_point,params)  
        if response.status_code==200:
            report_data=response.json()
            for i in report_data.items():
                
                groups = i[0].split("/")
                noshow_dict = i[1]
                for grp in range(len(groups)):
                    noshow_dict[f"group{grp}"] = groups[grp]
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

        df.drop_duplicates(subset=['userId','locationId','positionId'],inplace=True)

        column_list = get_column_names(NoShowsTemp)
        df = df[df.columns.intersection(column_list)]

        file_name = "upsert_noshows.sql"
        table_name = 'noshows'
        records = bulk_create(NoShowsTemp,df,noshows_temporary_table_query,file_name,table_name)
import unittest
import requests
import pyodbc

import datetime
from sqlalchemy import func
from models.models import *
from utility.constants import *
from utility.helper import bulk_create, get_column_names, convert_boolean_fields_to_lower
from utility.setting import *
from datetime import datetime
import pandas as pd
from utility.request import *
from utility.temporary_table_query import field_temporary_table_query
import numpy as np
from sqlalchemy import text
import math

session=Session()


TABLE1_NAME = 'field'
FIELD_TABLE_PRIMARY_KEY = 'id'  # Replace with your primary key for table1

class ETLTests(unittest.TestCase,Api):

    @classmethod
    def setUpClass(cls):
        cls.api_instance = Api()
        # Use the Api instance to make the request
        end_point = 'rest/api/3/field'           
        # cls.api_field_data = cls.api_instance.request(end_point).json()
        data = cls.api_instance.request(end_point).json()
        
        field_list = []
        for item in data:
            scope = item.get('scope',None)
            if scope:
                item['scope_type'] = scope['type']
                item['scope_project_id'] = scope['project']['id']
            else:
                item['scope_type'] = None
                item['scope_project_id'] = None

            schema = item.get('schema',None)
            if schema:
                item['schema_type'] = schema.get("type",None)
                item['schema_custom'] = schema.get('custom',None)
                item['schema_custom_id'] = schema.get("customId",None)
                item['schema_system']     = schema.get("system",None)
                item['schema_configuration'] = schema.get("configuration",None)
                item['schema_items'] = schema.get("items",None)
            else:
                item['schema_type'] = None
                item['schema_custom'] = None
                item['schema_custom_id'] = None
                item['schema_system']     = None
                item['schema_configuration'] = None
                item['schema_items'] = None

            field_list.append(item)

        # Create DataFrame
        df = pd.DataFrame(field_list)

        rename_columns = {
            "key":"field_key",
            "untranslatedName":"untranslated_name",
            "clauseNames":"clause_names",

        }
        df.rename(columns=rename_columns,inplace=True)

        column_list = get_column_names(FieldTemp)
        df = df[df.columns.intersection(column_list)]
        cls.api_field_data = df.to_dict("records")


    @classmethod
    def tearDownClass(cls):
        session.close()

    def convert_nan_to_none(self, value):
        return None if isinstance(value, float) and math.isnan(value) else value

    def test_primary_key_constraints_field_table(self):
        duplicates = session.execute(text(f"SELECT {FIELD_TABLE_PRIMARY_KEY}, COUNT(*) FROM {TABLE1_NAME} GROUP BY {FIELD_TABLE_PRIMARY_KEY} HAVING COUNT(*) > 1")).fetchall()
        try:
            self.assertEqual(len(duplicates), 0)
        except:
            print(f"Primary key constraint violated in {TABLE1_NAME}. Duplicates found: {duplicates}")



    def test_no_duplications_field_table(self):
        seen = set()
        duplicates = []
        for record in self.api_field_data:
            if record[FIELD_TABLE_PRIMARY_KEY] in seen:
                duplicates.append(record)
            seen.add(record[FIELD_TABLE_PRIMARY_KEY])
        try:
            self.assertEqual(len(duplicates), 0)
        except:
            print(f"Duplications found in API data for {TABLE1_NAME}: {duplicates}")


    def test_record_count_table1(self):
        api_count = len(self.api_field_data)
        result = session.execute(text(f"SELECT COUNT(*) FROM {TABLE1_NAME}")).scalar()
        try:
            self.assertEqual(api_count, result)
        except:
            print(f"Record count mismatch in {TABLE1_NAME}: API ({api_count}) vs DB ({result})")

    def test_record_field_values_table1(self):
        for record in self.api_field_data:
            result = session.execute(
                text(f"SELECT * FROM {TABLE1_NAME} WHERE {FIELD_TABLE_PRIMARY_KEY} = :pk"), {'pk': record[FIELD_TABLE_PRIMARY_KEY]}
            ).fetchone()
            try:
                self.assertIsNotNone(result)
            except:
                print(f"Record with primary key {record[FIELD_TABLE_PRIMARY_KEY]} not found in {TABLE1_NAME}")

            for field, value in record.items():
                if field != 'clause_names' and result:
                    db_value = result[field]
                    api_value = self.convert_nan_to_none(value)
                    db_value = self.convert_nan_to_none(db_value)
                
                try:
                    self.assertEqual(db_value, api_value)
                except:
                    print(f"Field value mismatch in {TABLE1_NAME} for {field}: API ({api_value}) vs DB ({db_value})")


if __name__ == '__main__':
    unittest.main()

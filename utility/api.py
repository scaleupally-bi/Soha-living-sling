import requests
import json
import os


class Api:
    def get(self,base_url,api,headers,params):
        response = requests.get(f"{str(base_url)+str(api)}",headers = headers,params = params)
        return response

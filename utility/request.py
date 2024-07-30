import os
import requests
from requests.auth import AuthBase
from utility.constants import REQUESTHEADERS

class Api():
    def __init__(self) -> None:
        self.password = os.getenv("password")
        self.email = os.getenv("email")
        self.base_url = os.getenv("BaseUrl")
        self.authorization_token = self.get_authorization_token()
        

        self.header = {
            "Content-Type": "application/json",  # Adjust content type as per API requirements
            "accept":"*/*",
            "Authorization":os.getenv("AUTHORIZATION_KEY")
        }
    
    def get_authorization_token(self):
        # Define the endpoint and credentials
        url = f"{self.base_url}account/login"
        credentials = {
            "email": self.email,
            "password": self.password
        }

        # Send the POST request
        response = requests.post(url, json=credentials)

        # Check if the request was successful
        if response.status_code == 200:
            # Fetch the authorization key from the headers
            auth_key = response.headers.get('Authorization')
            if auth_key:
                os.environ['AUTHORIZATION_KEY'] = auth_key
            else:
                print("Authorization key not found in headers.")
        else:
            print(f"Failed to login. Status code: {response.status_code}")
        
    def request(self, endpoint, params=None):
        # A GET request for an endpoint and the response the request will be returned
        url = f'{self.base_url}{endpoint}'
        response = requests.get(url, headers=self.header, timeout=120,params=params)
        return response
    


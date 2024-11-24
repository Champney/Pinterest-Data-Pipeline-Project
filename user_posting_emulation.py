import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import datetime
import yaml
random.seed(100)
db_creds_path = 'db_creds.yaml'
with open(db_creds_path, 'r') as file:
    db_creds = yaml.safe_load(file)
username = db_creds['AWS IAM Username']



class AWSDBConnector:

    def __init__(self):
        aws_db_creds_path = 'aws_db_creds.yaml'
        with open(aws_db_creds_path, 'r') as file:
            aws_db_creds = yaml.safe_load(file)
        self.HOST = aws_db_creds['HOST']
        self.USER = aws_db_creds['USER']
        self.PASSWORD = aws_db_creds['PASSWORD']
        self.DATABASE = aws_db_creds['DATABASE']
        self.PORT = aws_db_creds['PORT']
        db_creds_path = 'db_creds.yaml'
        with open(db_creds_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        self.api_stage = 'a-zA-Z0-9_'
        aws_username = db_creds['AWS IAM Username']
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                       
            pin_invoke_url = f"https://1yd64tc6fa.execute-api.us-east-1.amazonaws.com/{self.api_stage}/topics/{self.aws_username}.pin"
            geo_invoke_url = f"https://1yd64tc6fa.execute-api.us-east-1.amazonaws.com/{self.api_stage}/topics/{self.aws_username}.geo"
            user_invoke_url = f"https://1yd64tc6fa.execute-api.us-east-1.amazonaws.com/{self.api_stage}/topics/{self.aws_username}.user"
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            
            pin_schema = ["index", "unique_id", "title", "description", "poster_name",
             "follower_count", "tag_list", "is_image_or_video","image_src",
             "downloaded","save_location","category"]
            
            geo_schema = ["ind","timestamp","latitude","longitude","country"]

            user_schema = ["ind","first_name","last_name","age","date_joined"]

            
        pin_payload = json.dumps({
            "records": [
                {
                    "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], 
                    "title": pin_result["title"], "description": pin_result["description"],
                    "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], 
                    "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
                    "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], 
                    "save_location": pin_result["save_location"], "category": pin_result["category"]}
                }
            ]
        })
      
       
        pin_response = requests.request("POST", pin_invoke_url, headers=headers, data=pin_payload)

        geo_payload = json.dumps({
            "records": [
                {
               
                "value": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"], 
                          "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], 
                          "country": geo_result["country"]}
                }
            ]
        }, default=str)

        geo_response = requests.request("POST", geo_invoke_url, headers=headers, data=geo_payload)
        
        user_payload = json.dumps({
            "records": [
                { 
                "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], 
                "last_name": user_result["last_name"], "age": user_result["age"],
                "date_joined": user_result["date_joined"]}        
                        }
            ]
        }, default=str)


        user_response = requests.request("POST", user_invoke_url, headers=headers, data=user_payload)
        print(f"Pin: {pin_response}")
        print(f"Geo: {geo_response}")
        print(f"User: {user_response}")    


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    



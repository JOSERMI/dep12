import os
import yaml
from io import StringIO
from google.cloud.storage import Client
from azure.storage.blob import ContainerClient
from pymongo import MongoClient

with open('/user/app/ProyectoEndToEndPython/Proyecto/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

def get_cliente_cloud_storage():
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["credentials"]
    client = Client()
    
    return client


def get_cliente_azure_storage(containerName):

    conn_str = config["azure_storage"]["connection_string"]
    container = containerName
    container_client = ContainerClient.from_connection_string(
            conn_str=conn_str, 
            container_name=container
        )
    
    return container_client


def get_mongo_client(databaseName):
    
    CONNECTION_STRING = config["mongo_db"]["connection_string"]
    client = MongoClient(CONNECTION_STRING)
    dbname = client[databaseName]
    
    return dbname

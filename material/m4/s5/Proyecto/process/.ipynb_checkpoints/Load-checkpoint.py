import io
import os
from io import StringIO
import pandas as pd
from utils import utilitarios as u

class Load():
    
    def __init__(self) -> None:
        self.process = 'Load Process'

    def load_to_adls(self, df, containerName, blobName):

        container_client = u.get_cliente_azure_storage(containerName)
        output = io.StringIO()
        output = df.to_csv(encoding = "utf-8", index=False)
        container_client.upload_blob(blobName, output, overwrite=True, encoding='utf-8')
    
    def load_to_cloud_storage(self, df, bucketName, fileName): #bucketName= Datalake_krm, filename = "Alex/customer"
        
        client = u.get_cliente_cloud_storage()
        bucket = client.get_bucket(bucketName)
        bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv',)
    
    def load_to_mysql():
        pass

    def  load_to_bigquery():
        pass
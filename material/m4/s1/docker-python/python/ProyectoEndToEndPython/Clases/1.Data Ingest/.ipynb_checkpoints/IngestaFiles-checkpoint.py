import sqlalchemy as db
from sqlalchemy import text
from io import StringIO
import pandas as pd
import os

headers_files = {
    'customers':["customer_id","customer_fname","customer_lname","customer_email","customer_password",\
                 "customer_street","customer_city","customer_state","customer_zipcode"],
    'orders' : ["order_id","order_date","order_customer_id","order_status"],
    'order_items' : ["order_item_id","order_item_order_id","order_item_product_id","order_item_quantity",
                     "order_item_subtotal","order_item_product_price"]
}

def Extract():
    
    
    try:
        path = "./data"
        files = [file for file in os.listdir(path) if not file.startswith('.')] # Ignore hidden files
        for file in files:
            print(f"Inicia el Proceso de Ingesta - {file}")
            data = pd.read_csv(path+"/"+file, names=headers_files[file])
            Load(data,file)
    except Exception as e:
        print(f"Data Extract error: {e}" )
        
def Load(df,tbl_name):
    
    try:
        print(f"cargando la tabla - {tbl_name}")
        engine = db.create_engine("mysql://root:root@192.168.2.17:3310/retail_db_files")
        conn = engine.connect()
        df.to_sql(tbl_name, conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Data Load error: {e}" )
        
try:
    Extract()
except Exception as e:
    print(str(e))


from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

df_customers = extract.read_adls("datalake","landing/customers")
df_orders = extract.read_adls("datalake", "landing/orders") 
df_order_items = extract.read_adls("datalake", "landing/order_items") 
df_departments = extract.read_adls("datalake", "landing/departments") 
df_categories = extract.read_adls("datalake","landing/categories") 
df_products = extract.read_adls("datalake","landing/products") 

df_enunciado1 = transform.enunciado1(df_customers, df_orders, df_order_items)
df_enunciado2 = transform.enunciado2(df_order_items, df_products,df_categories)
df_enunciado3 = transform.enunciado3(df_customers, df_orders, df_order_items, df_products, df_categories)

load.load_to_adls(df_enunciado1,"datalake", "gold/df_enunciado1")
load.load_to_adls(df_enunciado2,"datalake", "gold/df_enunciado2")
load.load_to_adls(df_enunciado3,"datalake", "gold/df_enunciado3")
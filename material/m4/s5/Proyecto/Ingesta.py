from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

argm = arg[]

def ingest(extract, source, foler)
    df = extract.read_adls(source,foler)
    return df

def load(load, df, sink, folder):
    load.load_to_adls(df, sink, folder)
    
df = ingest(extract, argm['source'], argm['folder'])

load(load, df, argm['sink'], argm['folder'])

print("INICIA EXTRACION DE DATOS")

print("Customers")
df_customers = extract.read_adls("source","retail/customers")


print("Orders")
df_orders = extract.read_adls("source", "retail/orders")


print("TERMINA EXTRACION DE DATOS")

print("INICIA CARGA DE DATOS A LANDING")

load.load_to_adls(df_customers, 'datalake', 'landing/customers')
load.load_to_adls(df_orders, 'datalake','landing/orders')

print("TERMINA CARGA DE DATOS A LANDING")


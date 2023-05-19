import mysql
import pandas as pd
import mysql.connector as connection
 
import warnings
warnings.filterwarnings('ignore')

def list_mysql_tables(request):
 
  headers = {
      'Access-Control-Allow-Origin': '*',
      'Content-Type': 'application/json'
  }

  db = mysql.connector.connect(
    host="72.167.124.128",
    user="user_test2023",
    password="g&d*nf!12vQj",
    database="db_test2023"
  )
   
  query = '''SELECT
	            table_schema
              , table_name
              , table_rows
            FROM information_schema.tables
            WHERE TABLE_TYPE = "BASE TABLE"'''
 
  result_dataframe = pd.read_sql(query, db)
  db.close()
  
  return (result_dataframe.to_json(orient='records'), 200, headers)
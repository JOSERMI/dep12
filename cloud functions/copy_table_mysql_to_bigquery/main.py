import os
import json
import pandas as pd
import mysql
import mysql.connector as connection
from google.cloud import bigquery

import warnings
warnings.filterwarnings('ignore')


def copy_table_mysql_to_bigquery(request):
  table_source = request.args['table_source']
  table_destination = request.args['table_destination']
  project_id = 'test-prj-data-911'
  schema = 'raw'
  table_id =  '{0}.{1}.{2}'.format(project_id, schema, table_destination)

  headers = {
      'Access-Control-Allow-Origin': '*',
      'Content-Type': 'application/json'
  }

  if table_source.strip() == "" or table_destination.strip() == "":
    return (json.dumps({'result': 'error', 'description': 'no table name'}), 200, headers)
  
  db = mysql.connector.connect(
    host="72.167.124.128",
    user="user_test2023",
    password="g&d*nf!12vQj",
    database="db_test2023"
  )
   
  query = 'SELECT * FROM {0}'.format(table_source)
  result_dataframe = pd.read_sql(query, db)
  db.close()
  
  client = bigquery.Client()

  job_config = bigquery.LoadJobConfig(
      write_disposition="WRITE_TRUNCATE",
  )


  job = client.load_table_from_dataframe(
    result_dataframe, table_id, job_config=job_config
  )
  job.result()

  try:
      client.get_table(table_id)
  except NotFound:
      return (json.dumps({'result': 'error', 'description': 'table not found'}), 200, headers)

  return (json.dumps({'result': 'ok'}), 200, headers)
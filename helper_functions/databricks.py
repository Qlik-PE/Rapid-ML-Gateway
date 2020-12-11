import os
import requests
import pandas as pd
from io import StringIO

def score_model(dataset: pd.DataFrame, url,  headers):
  #url = 'https://adb-3430516339914093.13.azuredatabricks.net/model/wine_quality/2/invocations'
  # = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  print('test')
  print(url)
  print(headers)
  data_json = dataset.to_dict(orient='split')
  print(data_json)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

def convert_to_df(string, metadata):
  print("entering connvert to df")
  print(type(string))
  print(metadata)
  #print(type(df))
  #print(df)
  #df.columns= metadata
  df = pd.read_csv(StringIO(string), sep=",")
  print(df.columns)
  print("exiting convert to df")
  return df
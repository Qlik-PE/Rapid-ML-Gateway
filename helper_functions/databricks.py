import os
import requests
import pandas as pd

def score_model(dataset: pd.DataFrame, url,  headers):
  #url = 'https://adb-3430516339914093.13.azuredatabricks.net/model/wine_quality/2/invocations'
  #headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split')
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()
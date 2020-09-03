import json
import logging
import logging.config
import csv
import os, sys, inspect, time
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PARENT_DIR, 'helper_functions'))

import requests
import qlist

def get_tables(url):
    tables_url = url + 'tables/'
    logging.debug(tables_url)
    resp = requests.get(tables_url)
    tables_dict = resp.json()
    return tables_dict, resp

def get_table_id(value, url):
    tables_url = url + 'tables/'
    logging.debug(tables_url)
    resp = requests.get(tables_url)
    tables_dict = resp.json()
    table_id = qlist.get_table_id(tables_dict, value)
    return table_id, resp

def get_table_information(table_id, url):
        table_url = url +'table/' + table_id
        resp = requests.get(table_url)
        table_dict = resp.json()
        #print(type(table_dict)) 
        columns = table_dict['columns']
        #print(type(columns))
        #The Keys of Dictionary  (['name', 'description', 'query', 'meta', 'columns'])
        return table_dict, columns, resp
def get_column_info(value, url):
    table_id = get_table_id(value, url)[0]
    column_data = get_table_information(table_id, url)[1]
    return column_data

def get_access_tokens(table_id, url):
        token_url = url +'table/' + table_id + '/access-tokens'
        #print(token_url)
        resp = requests.get(token_url)
        token_dict = resp.json()
        return token_dict, resp


def cleanup_token(token, table_id, url):
    token_url =url+ 'table/' + table_id + '/access-token/' + token
    resp = requests.delete(token_url)
    return resp

def clear_all_tokens(url):
    token_url = url +'tokens'
    resp = requests.delete(token_url)
    return resp

def get_count_of_all_tokens(url):
    token_url = url +'tokens'
    resp = requests.get(token_url)
    token_dict = resp.json()
    return token_dict, resp

def create_token(url, table_id):
    create_token_url = url +'table/' + table_id + '/access-token?expiresInSec=3000'
    logging.debug("Create Token URL {}" .format(create_token_url))
    resp = requests.post(create_token_url)
    new_token_dict = resp.json()
    access_token = new_token_dict["id"]
    secret = new_token_dict["secret"]
    return access_token, secret, resp

def get_result_csv(url, secret):
    create_token_url = url+'result/'+secret+".csv"
    #print(create_token_url)
    resp = requests.get(create_token_url)
    resp.encoding = "utf-8-sig"
    return resp, resp.text

def get_result_json(url, secret):
    create_token_url = url+'result/'+secret+".ldjson"
    resp = requests.get(create_token_url)
    resp.encoding = "utf-8-sig"
    return resp, resp.text

def convert_csv(input_str):
    lines = input_str.splitlines()
    reader = csv.reader(lines)
    parsed_csv = list(reader)
    header = parsed_csv[0]
    parsed_csv.pop(0)
    return parsed_csv, header
import os
import re
import json
import numpy as np
import pandas as pd
from google.oauth2 import service_account
import google.auth
import google.auth.transport.requests
from googleapiclient.discovery import build
from google.cloud import bigquery,storage
from google.cloud import bigquery_v2
from datetime import date,timedelta,datetime
import time
import requests

def validate_project(client,proj_id):
    try:
        datasets = client.list_datasets(proj_id)
        return True
    except Exception:
        # raise ValueError('The project id {} is not valid'.format(proj_id))
        return False

def validate_dataset(client,ds_id):
    try:
        tables = client.get_dataset(ds_id)
        return True
    except Exception:
        # raise ValueError('The dataset id {} is not valid'.format(ds_id))
        return False

def validate_table(client,tbl_id):
    try:
        table = client.get_table(tbl_id)
        return True
    except Exception:
        # raise ValueError('The dataset id {} is not valid'.format(ds_id))
        return False

def get_tbl_prop(client, tbl_id):
    try:
        tbl = client.get_table(tbl_id)
    except Exception:
        raise ValueError('Failed to get metadata of table {}'.format(tbl_id))
    tbl_properties = {}
    tbl_properties['location'] = tbl.location
    tbl_properties['num_bytes'] = tbl.num_bytes
    tbl_properties['num_rows'] = tbl.num_rows
    tbl_properties['view_query']=tbl.view_query
    tbl_properties['mview_query'] = tbl.mview_query
    tbl_properties['table_type'] = tbl.table_type
    if tbl.table_type == 'TABLE':
       tbl_properties['table_and_view'] = 'table' #integer partition
    else:
        tbl_properties['table_and_view'] = 'view' # not table

    return tbl_properties

if __name__ == '__main__':
    # Value to be changed
    # name= 'events_20200901'
    # table_pattern = '^events_'+ '[0-9]{8}$'
    # pattern = re.compile(table_pattern, re.IGNORECASE)
    # found = pattern.findall(name)

    path_to_credential = '/Users/wangez/Downloads/allen-first-8d361c053705.json'
    sheet_url = 'https://docs.google.com/spreadsheets/d/1u64Ig0w6Vk7zYEVGs-QSCg1eQtrlcT26kvMe6TuXbpw/edit?usp=sharing&resourcekey=0-S1BJoHiMuQkKMMFp_JplLw'
    getid = '^.*/d/(.*)/.*$'
    pattern = re.compile(getid, re.IGNORECASE)
    sheet_id = pattern.findall(sheet_url)[0]
    range1 = 'Config'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    scope= [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    credentials = service_account.Credentials.from_service_account_file(path_to_credential,scopes=scope)
    auth_req=google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token=credentials.token

    sheet_service = build("sheets", "v4", credentials=credentials)
    gsheet = sheet_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range1).execute()
    header = gsheet.get('values', [])[0]  # First line is header!
    values = gsheet.get('values', [])[1:]  # Data is 2-dimension array list

    client = bigquery.Client()
    gcs_client=storage.Client()
    row_num=2
    list_ops_by_table=[]
    print('----------------Phase 1 - Parsing Input ----------------')
    for row in values:
        print('Parsing row {}'.format(str(row_num)))

        if len(row) < len(header):
            for index in range(len(header) - len(row)):
                row.append('')

        input_project = row[0]
        if input_project == '' or input_project == '*':
            raise ValueError("Error in row {} - Project can not be null or *".format(str(row_num)))
        else:
            if not validate_project(client,input_project):
                raise ValueError("Error in row {} - Project is not valid ".format(str(row_num)))


        input_dataset=row[1]
        if input_dataset == '':
            raise ValueError("Error in row {} - Dataset can not be null".format(str(row_num)))
        else:
            if input_dataset != '*':
                ds_id=input_project + '.' + input_dataset
                if not validate_dataset(client,ds_id):
                    raise ValueError("Error in row {} - dataset is not valid ".format(str(row_num)))


        input_table = row[2]
        if input_dataset != '*' and input_table == '':
            raise ValueError("Error in row {} - table can not be null".format(str(row_num)))
        else:
            if input_dataset !='*' and input_table !='' :
                tbl_id = input_project + '.' + input_dataset + '.' + input_table
                if not validate_table(client,tbl_id) and not '*' in tbl_id:
                    raise ValueError("Error in row {} - table is not valid ".format(str(row_num)))

        dest_proj= row[3]
        dest_dataset = row[4]
        dest_table=row[5]

        if input_dataset == '*':
            datasets = client.list_datasets(input_project)
            if datasets:
                for dataset in datasets:
                    ds_id=input_project + '.' + dataset.dataset_id
                    tables = client.list_tables(ds_id)
                    if tables:
                        for table in tables:
                            tbl_id=ds_id + '.' + table.table_id
                            tbl_properties=get_tbl_prop(client,tbl_id)
                            dict_ops_by_table = {
                                'input_project':input_project,
                                'input_dataset':dataset.dataset_id,
                                'input_table':table.table_id,
                                'tbl_id':tbl_id,
                                'tbl_properties':tbl_properties,
                                'dest_project':dest_proj,
                                'dest_dataset':dest_dataset,
                                'dest_table':dest_table
                            }
                            list_ops_by_table.append(dict_ops_by_table)
                    else:
                        print('No tables found in dataset {}'.format(ds_id))
            else:
                print('No dataset found in project {}'.format(input_project))
        elif input_table == '*':
            ds_id = input_project + '.' + input_dataset
            tables = client.list_tables(ds_id)
            if tables:
                for table in tables:
                    tbl_id = ds_id + '.' + table.table_id
                    tbl_properties = get_tbl_prop(client, tbl_id)
                    dict_ops_by_table = {
                        'input_project': input_project,
                        'input_dataset': input_dataset,
                        'input_table': table.table_id,
                        'tbl_id':tbl_id,
                        'tbl_properties': tbl_properties,
                        'dest_project': dest_proj,
                        'dest_dataset': dest_dataset,
                        'dest_table': dest_table
                    }
                    list_ops_by_table.append(dict_ops_by_table)
            else:
                print('No tables found in dataset {}'.format(ds_id))
        else:
            tbl_id=input_project + '.' + input_dataset + '.' + input_table
            tbl_properties = get_tbl_prop(client, tbl_id)
            dict_ops_by_table = {
                'input_project': input_project,
                'input_dataset': input_dataset,
                'input_table': input_table,
                'tbl_id':tbl_id,
                'tbl_properties': tbl_properties,
                'dest_project': dest_proj,
                'dest_dataset': dest_dataset,
                'dest_table': dest_table
            }
            list_ops_by_table.append(dict_ops_by_table)
        print('Complete parsing row {}'.format(str(row_num)))
        row_num += 1
    list_ops_by_phy_table = [x for x in list_ops_by_table if x['tbl_properties']['table_and_view'] == 'table']
    print('Complete parsing {} inputs and find {} physical tables'.format(str(len(values)),str(len(list_ops_by_phy_table))))

    print('----------------Check Duplicate table----------------')
    seen={}
    for item in list_ops_by_phy_table:
        if item['tbl_id'] in seen.keys():
            seen[item['tbl_id']] +=1
        else:
            seen[item['tbl_id']]=1
    duplicate_items = {k:v for k,v in seen.items() if v>1}
    if len(duplicate_items) > 0:
        print(duplicate_items)
        raise ValueError('There are duplicate tables specified in the input files, please check in the input')

    print('----------------Phase 2 - Processing Tables ----------------')
    for item in list_ops_by_phy_table:
        input_project=item['input_project']
        input_dataset=item['input_dataset']
        input_table=item['input_table']
        location=item['tbl_properties']['location']
        dest_project=item['dest_project']
        dest_dataset=item['dest_dataset']
        dest_table=item['dest_table']
        if dest_project == '':
            dest_project=input_project

        if dest_dataset=='':
            dest_dataset=input_dataset + '_snapshot'
            snap_dataset_id=dest_project + '.' + dest_dataset
            if not validate_dataset(client,snap_dataset_id):
                snap_dataset=client.dataset(snap_dataset_id)
                snap_dataset.location =location
                snap_dataset=client.create_dataset(snap_dataset_id,timeout=30)
        else:
            snap_dataset_id = dest_project + '.' + dest_dataset
            if not validate_dataset(snap_dataset_id):
                raise ValueError('Destination dataset {} is not valid.'.format(snap_dataset_id))

        if dest_table == '':
            dest_table=input_table + '_' + date.today().strftime('%Y%m%d')
        dest_tbl_id=dest_project + '.' + dest_dataset + '.' + dest_table
        if validate_table(client,dest_tbl_id):
                raise ValueError('Table {} already exist, please give a new table name.'.format(dest_table))

        bq_endpoint = 'https://bigquery.googleapis.com/bigquery/v2/projects/{}/jobs'.format(input_project)

        request_body=json.dumps({
                      "configuration": {
                        "copy": {
                          "sourceTables": [
                            {
                              "projectId": input_project,
                              "datasetId": input_dataset,
                              "tableId": input_table
                            }
                          ],
                          "destinationTable": {
                            "projectId": dest_project,
                            "datasetId": dest_dataset,
                            "tableId": dest_table
                          },
                          "operationType": "SNAPSHOT",
                          "writeDisposition": "WRITE_TRUNCATE",
                        }
                      }
                    })

        headers = {'Authorization': 'Bearer {}'.format(token)}
        response = requests.post(bq_endpoint, headers=headers,data=request_body)
        src_tbl_id = input_project + '.' + input_dataset + '.' + input_table
        dest_tbl_id = dest_project + '.' + dest_dataset + '.' + dest_table
        if response.status_code ==200:
            job_id = json.loads(response.content.decode("utf-8"))['id'].split('.')[1]
            print('Snapshot {} for table {} submitted with jobid {}'.format(dest_tbl_id,src_tbl_id,job_id))
            snapshot_job=client.get_job(job_id)
            job_state=snapshot_job.state
            job_created = snapshot_job.created
            wait_index=1
            while job_state == 'RUNNING':
                print('waiting for {} seconds'.format(str(wait_index * 5)))
                time.sleep(5)
                snapshot_job = client.get_job(job_id)
                job_state = snapshot_job.state
                wait_index += 1
            job_ended = snapshot_job.ended
            job_duration = job_ended - job_created
            print('Job {} completed in {}'.format(job_id,str(job_duration)))
        else:
            print('Snapshot {} for table {} failed'.format(dest_tbl_id, src_tbl_id))








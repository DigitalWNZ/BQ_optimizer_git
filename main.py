import os
import re
import json
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery,storage
from google.cloud import bigquery_v2
from datetime import date,timedelta,datetime
import time

def validate_project(client,proj_id):
    try:
        datasets = client.list_datasets(proj_id)
        return True
    except Exception:
        # raise ValueError('The project id {} is not valid'.format(proj_id))
        return False

def validate_dataset(client,ds_id):
    try:
        tables = client.list_tables(ds_id)
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
    tbl_properties['partitioning_type'] = tbl.partitioning_type
    tbl_properties['time_partitioning'] = tbl.time_partitioning
    tbl_properties['range_partitioning'] = tbl.range_partitioning
    tbl_properties['view_query']=tbl.view_query
    tbl_properties['mview_query'] = tbl.mview_query
    tbl_properties['table_type'] = tbl.table_type
    if tbl.table_type == 'TABLE':
        if tbl.partitioning_type is not None or tbl.range_partitioning is not None:
            tbl_properties['table_and_partition'] = 3
        else:
            tbl_properties['table_and_partition'] = 2
    else:
        tbl_properties['table_and_partition'] = 1

    return tbl_properties

def bucket_exist(project,bucket_name):
    found_bucket=False
    buckets = gcs_client.list_buckets(project=project)
    for bucket in buckets:
        if bucket.name == bucket_name:
            found_bucket = True
    return found_bucket

def export_table_partitions(client, tbl_id,table_item,gcs_client):
    start_partition = date.today() - timedelta(days=table_item['days_before_export'])
    partitions = client.list_partitions(tbl_id)
    # the following two lines is to get table id
    # dataset_ref = bigquery.DatasetReference(table_item['input_project'], table_item['input_dataset'])
    # table_ref = dataset_ref.table(table_item['input_table'])

    if len(partitions) > 0:
        bucket_name = table_item['gcs_dest_bucket']

        if bucket_exist(table_item['input_project'],bucket_name):
            gcs_url='gs://{}/{}/{}/dt='.format(bucket_name,table_item['input_dataset'],table_item['input_table'])
        else:
            new_bucket_name=table_item['input_project'] + '-external-data'
            if not bucket_exist(table_item['input_project'], new_bucket_name):
                gcs_client.create_bucket(new_bucket_name,location=table_item['tbl_properties']['location'],project=table_item['input_project'])
                ###TODO: apply policy
            gcs_url = 'gs://{}/{}/{}/dt='.format(new_bucket_name, table_item['input_dataset'], table_item['input_table'])

        for partition in partitions:
            dt_partition=datetime.strptime(partition,'%Y%m%d').date()
            gcs_url=gcs_url + '{}/*.avro'.format(str(dt_partition))
            if dt_partition <= start_partition:
                print('Extracting partition {}${} to {}'.format(tbl_id,partition,gcs_url))
                try:
                    extract_job = client.extract_table(tbl_id + '$' +partition,gcs_url,location=table_item['tbl_properties']['location'])
                    job_created=extract_job.created
                    job_id=extract_job.job_id
                    job_state=extract_job.state
                    print('Extracting partition {}${}, the job id is {}'.format(tbl_id, partition,job_id))
                    wait_index=1
                    while job_state == 'RUNNING':
                        print('waiting for {} seconds'.format(str(wait_index * 10)))
                        time.sleep(10)
                        extract_job=client.get_job(job_id)
                        job_state=extract_job.state
                        wait_index +=1
                    job_ended = extract_job.ended
                    job_duration = job_ended - job_created
                    print('The job completed in {}'.format(str(job_duration)))
                except Exception:
                    raise Exception('Something went wrong when exporting partition {}${}'.format(tbl_id,partition))

                if table_item['name_ext_table'] !='':
                    if not ('.' in name_ext_table):
                        raise ValueError('Please use full table name for external table {}'.format(table_item['name_ext_table']))

                    if not validate_table(client,table_item['name_ext_table']):
                        name_ext_table = table_item['input_project'] + '.' + table_item['input_dataset'] + '.' + table_item['input_table'] + '_ext'
                        if not validate_table(client,name_ext_table):
                            continue




def create_external_table(client,table_item):
    exit(0)


if __name__ == '__main__':

    # Value to be changed
    path_to_credential = '/Users/wangez/Downloads/allen-first-1b9a548ebc7b.json'
    sheet_url = 'https://docs.google.com/spreadsheets/d/1KbkOtCZ9voavehT44X8GMtjiTE7NWxc7FN52gsEsxX3E/edit#gid=0'

    getid = '^.*/d/(.*)/.*$'
    pattern = re.compile(getid, re.IGNORECASE)
    sheet_id = pattern.findall(sheet_url)[0]
    range1 = 'TableSpec'
    event_json = {}
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    credentials = service_account.Credentials.from_service_account_file(path_to_credential)
    scopes = credentials.with_scopes(
        [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
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
            raise ValueError("Error in row {} - partitioned_table can not be null".format(str(row_num)))
        else:
            if input_dataset !='*' and input_table !='' and input_table != '*':
                tbl_id = input_project + '.' + input_dataset + '.' + input_table
                if not validate_table(client,tbl_id):
                    raise ValueError("Error in row {} - table is not valid ".format(str(row_num)))

        days_before_remove = row[3]
        try:
            if days_before_remove == '':
                days_before_remove = 0
            else:
                days_before_remove=int(row[3])
        except ValueError:
            raise ValueError(
                'days_before_remove in row {} can not be converted into integer.'.format(str(row_num)))

        days_before_export = row[4]
        try:
            if days_before_export == '':
                days_before_export = 0
            else:
                days_before_export = int(row[4])
        except ValueError:
            raise ValueError(
                'days_before_export in row {} can not be converted into integer.'.format(str(row_num)))

        remove_orig_data=row[5]
        if remove_orig_data != 'Y' and remove_orig_data !='N':
            raise ValueError('remove original in row {} data should only be Y or N'.format(str(row_num)))


        gcs_dest_bucket=row[6]
        if gcs_dest_bucket != '':
            if gcs_dest_bucket.startswith('gs://'):
                gcs_dest_bucket=gcs_dest_bucket[5:]
                # raise ValueError('gcs destination bucket in row {} is not a valid gs url'.format(str(row_num)))
            if gcs_dest_bucket.endswith('/'):
                gcs_dest_bucket = gcs_dest_bucket[:-1]
            if '/' in gcs_dest_bucket:
                raise ValueError('Only bucket name is needed in row {}'.format(str(row_num)))

        name_ext_table=row[7]

        gcs_lifecycle_policy=row[8]
        try:
            if gcs_lifecycle_policy == '':
                gcs_lifecycle_policy = {}
            else:
                gcs_lifecycle_policy=json.loads(gcs_lifecycle_policy)
        except ValueError:
            raise ValueError('gcs life cycle policy in row {} is not a valid json'.format(str(row_num)))

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
                                'tbl_properties':tbl_properties,
                                'days_before_remove':days_before_remove,
                                'days_before_export':days_before_export,
                                'remove_orig_data':remove_orig_data,
                                'gcs_dest_bucket':gcs_dest_bucket,
                                'name_ext_table':name_ext_table,
                                'gcs_lifecycle_policy':gcs_lifecycle_policy
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
                        'tbl_properties': tbl_properties,
                        'days_before_remove': days_before_remove,
                        'days_before_export': days_before_export,
                        'remove_orig_data': remove_orig_data,
                        'gcs_dest_bucket': gcs_dest_bucket,
                        'name_ext_table': name_ext_table,
                        'gcs_lifecycle_policy': gcs_lifecycle_policy
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
                'tbl_properties': tbl_properties,
                'days_before_remove': days_before_remove,
                'days_before_export': days_before_export,
                'remove_orig_data': remove_orig_data,
                'gcs_dest_bucket': gcs_dest_bucket,
                'name_ext_table': name_ext_table,
                'gcs_lifecycle_policy': gcs_lifecycle_policy
            }
            list_ops_by_table.append(dict_ops_by_table)
        print('Complete parsing row {}'.format(str(row_num)))
        row_num += 1
    list_ops_by_phy_table = [x for x in list_ops_by_table if x['tbl_properties']['table_and_partition'] >= 2]
    list_ops_by_par_table = [x for x in list_ops_by_table if x['tbl_properties']['table_and_partition'] == 3]
    print('Complete parsing {} inputs and find {} physical tables in which there are {} partition tables' \
          .format(str(len(values)),str(len(list_ops_by_phy_table)),str(len(list_ops_by_par_table))))

    print('----------------Phase 2 - Processing Tables ----------------')
    for item in list_ops_by_par_table:
        tbl_id=item['input_project'] + '.' + item['input_dataset'] + '.' + item['input_table']
        print('Processing table {}'.format(tbl_id))

        if item['days_before_remove'] > 0:
            # Remove partition
            # remove_table_partitions(client,tbl_id,item)
            continue
        elif item['days_before_export'] > 0:
            export_table_partitions(client, tbl_id, item,gcs_client)
        else:
            continue

#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 29/10/20 2:37 pm

@author: David Wong
"""
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

import logging
import numpy as np
import os
import pandas as pd
import pendulum
import pytz

from conf import default_args
from util import db_util

project_name = 'w0-external_traefik'
default_args['start_date'] = datetime(2020, 9, 26, 15, 0, 0)

dag_params = {
    'dag_id': project_name,
    'default_args': default_args,
    'start_date': datetime(2020, 9, 26, 15, 0, 0),
    'catchup': True,
    'schedule_interval': '@daily',
    'concurrency': 1
}

execution_date = '{{ execution_date }}'


def parse_str(string):
    try:
        return string.strip('"')
    except:
        return None


def parse_datetime(x):
    """
    Parses datetime with timezone formatted as:
        `[day/month/year:hour:minute:second zone]`

    Example:
        `>>> parse_datetime('13/Nov/2015:11:45:42 +0000')`
        `datetime.datetime(2015, 11, 3, 11, 45, 4, tzinfo=<UTC>)`

    Due to problems parsing the timezone (`%z`) with `datetime.strptime`, the
    timezone will be obtained using the `pytz` library.
    """
    try:
        dt = datetime.strptime(x[1:-7], '%d/%b/%Y:%H:%M:%S')
        dt_tz = int(x[-6:-3])*60+int(x[-3:-1])
        return dt.replace(tzinfo=pytz.FixedOffset(dt_tz))
    except:
        return None


def parse_duration(duration):
    try:
        return int(duration.strip('ms'))
    except:
        return 0


with DAG(**dag_params) as dag:

    lakes = ['drupal', 'yii']

    def etl_logs(s3_conn_id, s3_bucket, conn_id, exec_date):

        FIELDS = {
            'datetime': 'timestamptz NOT NULL',
            'ip': 'varchar NOT NULL',
            'request': 'varchar NOT NULL',
            'status': 'int4 NOT NULL',
            'size': 'int8 NOT NULL',
            'referer': 'varchar NULL',
            'user_agent': 'varchar NULL',
            'router_name': 'varchar NULL',
            'server_url': 'varchar NULL',
            'request_duration': 'int8 NOT NULL'
        }

        raw_table = 'w0_external.traefik'
        summary_table = 'w0_external.traefik_minute'

        raw_table_retention = '2 WEEK'
        summary_table_retention = '1 YEAR'

        def get_max_id(conn_id):
            query = f'''
            SELECT max(id)
            FROM {raw_table}
            '''

            return db_util.exec_sql(conn_id, query)[0][0]

        def consolidate_traffic(conn_id, max_id):
            query = f'''
            INSERT INTO {summary_table}
            AS a(SELECT 
                DATE_TRUNC('minute', datetime) AS datetime, 
                router_name,
                COUNT(*) AS hit_count
            FROM w0_external.traefik 
            WHERE router_name IS NOT NULL 
            AND id > {max_id}
            GROUP BY 1, 2)
            ON CONFLICT (datetime, router_name) DO UPDATE
            SET datetime = a.datetime, router_name = a.router_name, hit_count = a.hit_count
            '''

            db_util.exec_sql(conn_id, query, fetch=False)

        def prune_tables(conn_id):
            prune_raw_table = f'''
            DELETE FROM {raw_table}
            WHERE datetime < now() - INTERVAL '{raw_table_retention}'
            '''
            db_util.exec_sql(conn_id, prune_raw_table, fetch=False)

            prune_summary_table = f'''
            DELETE FROM {summary_table}
            WHERE datetime < now() - INTERVAL '{summary_table_retention}'
            '''
            db_util.exec_sql(conn_id, prune_summary_table, fetch=False)

            pass

        def extract(s3_conn_id, s3_bucket, exec_date):
            s3 = S3Hook(s3_conn_id)

            date_str = pendulum.parse(exec_date).to_date_string()

            path = 'hre-prod/traefik'
            file = f'access.log.{date_str}'
            key = f'{path}/{file}'

            logging.info(f'Retrieving {key} from {s3_bucket}')
            data = s3.get_key(key, bucket_name=s3_bucket).get()['Body'].read()

            with open(file, 'wb') as f:
                f.write(data)

            try:
                logging.info('Parsing CSV File')
                df = pd.read_csv(
                    file,
                    sep=r'(?<!,)\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])',
                    engine='python',
                    usecols=[0, 3, 4, 5, 6, 7, 8, 10, 11, 12],
                    names=['ip', 'datetime', 'request', 'status', 'size', 'referer', 'user_agent', 'router_name',
                           'server_url', 'request_duration'],
                    na_values='-',
                    header=None,
                    dtype={'status': pd.Categorical},
                    converters={
                        'request_duration': parse_duration,
                        'datetime': parse_datetime,
                        'request': parse_str,
                        'referer': parse_str,
                        'user_agent': parse_str,
                        'router_name': parse_str,
                        'server_url': parse_str,
                    },
                    error_bad_lines=False
                ).replace([np.nan], [None])

                docs = df.to_dict('records')

            except Exception as e:
                print(e)
                raise
            finally:
                os.unlink(file)

            return docs

        def transform(docs):
            return [doc for doc in docs if doc['datetime'] is not None]

        def load(conn_id, docs):
            logging.info(f'Loading into {raw_table}')
            db_util.pg_load(conn_id, docs, raw_table, FIELDS)
            logging.info(f'Loaded {len(docs)} Rows.')

        max_id = get_max_id(conn_id)
        logging.info(f'Retrieved Max ID of {max_id}')

        docs = extract(s3_conn_id, s3_bucket, exec_date)
        docs_t = transform(docs)
        load(conn_id, docs_t)
        logging.info(f'Error Rows: {len(docs) - len(docs_t)}')

        logging.info('Generating Summary Table')
        consolidate_traffic(conn_id, max_id)

        logging.info('Pruning Tables of Excess Data')
        prune_tables(conn_id)
        logging.info('Done')


    load_traefik_logs = PythonOperator(
            task_id='etl',
            python_callable=etl_logs,
            op_kwargs={
                's3_conn_id': 'datalake_s3',
                's3_bucket': 'hre-logs',
                'conn_id': 'postgres_prod',
                'exec_date': execution_date
            }
        )

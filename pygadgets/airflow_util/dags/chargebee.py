#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 10/2/21 4:06 pm

@author: David Wong
"""
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from conf import default_args

import chargebee
import time
import json
import logging

from datetime import datetime
from util import db_util

project_name = 'w0_chargebee'
default_args['start_date'] = datetime(2015, 7, 23)

dag_params = {
    'dag_id': project_name,
    'default_args': default_args,
    'start_date': datetime(2015, 7, 22, 9, 0, 0),
    'catchup': False,
    'schedule_interval': '@daily',
}

with DAG(**dag_params) as dag:

    def get_last_sync_date(conn_id, sitename, module):
        query = f'''
        SELECT updated_at
        FROM etl_log.chargebee
        WHERE
            sitename = '{sitename}' AND
            module = '{module}'
        '''

        try:
            return db_util.exec_sql(conn_id, query, fetch=True)[0][0]
        except:
            return None

    def get_keys(lst):

        data_mapping = {
            str: 'text',
            int: 'int8',
            float: 'double precision',
            bool: 'bool'
        }

        json_columns = {
            'linked_invoices', 'line_items', 'discounts', 'line_item_discounts', 'line_item_tiers', 'linked_payments',
            'dunning_attempts', 'applied_credits', 'adjustment_credit_notes', 'linked_orders', 'billing_address',
            'notes', 'addons', 'coupons', 'relationship', 'parent_account_access', 'child_account_access', 'payment_method'
        }

        dct = {k: data_mapping[type(v)] for e in lst for k, v in e.items()}

        for column in json_columns:
            if dct.get(column):
                dct[column] = 'jsonb'

        return dct

    def extract(module, batch_size=100, last_sync_date=None):

        lst, res = [], None
        obj = getattr(chargebee, module)

        args = {
            'limit': batch_size,
            'sort_by[asc]': 'updated_at',
            'offset': None
        }

        if last_sync_date is None:
            args['updated_at[before]'] = int(time.time())
        else:
            args['updated_at[after]'] = last_sync_date

        while res is None or args['offset'] is not None:
            res = obj.list(args)
            lst += [getattr(e, module.lower()).values for e in res]
            args['offset'] = res.next_offset

        return lst

    def transform(lst):
        for k1, v1 in enumerate(lst):
            for k2, v2 in v1.items():
                if type(v2) in (list, dict):
                    lst[k1][k2] = json.dumps(v2)

        return lst

    def load(conn_id, docs, table):

        def generate_create_statement(lst, table):

            def retrieve_table_schema(conn_id, table):
                schema, table_name = table.split('.')
                query = f'''
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name.lower()}' 
                '''

                return set(e[0] for e in db_util.exec_sql(conn_id, query))

            keys = get_keys(lst)
            existing_columns = retrieve_table_schema(conn_id, table)
            if len(existing_columns) == 0:

                columns = [f'''{k} {v}''' for k, v in keys.items()]
                column_str = ', '.join(columns)

                stmt = f'''
                CREATE TABLE IF NOT EXISTS {table} 
                ({column_str},
                PRIMARY KEY (id))
                '''

            else:
                keys = {k:v for k, v in keys.items() if k not in existing_columns}

                if len(keys) > 0:
                    columns = [f'''ADD COLUMN {k} {v}''' for k, v in keys.items()]
                    column_str = ', '.join(columns)

                    stmt = f'''
                    ALTER TABLE {table}
                    {column_str}
                    '''

                else:
                    stmt = 'SELECT now()'

            return stmt

        db_util.exec_sql(conn_id, generate_create_statement(docs, table), fetch=False)
        db_util.pg_load(conn_id, docs, table, get_keys(docs), constraints=['id'])

    def update_etl_log(conn_id, docs, sitename, module):
        fields = {
            'sitename': 'varchar',
            'module': 'varchar',
            'updated_at': 'int8'
        }

        last_updated = max(e['updated_at'] for e in docs)

        dct = {
            'sitename': sitename,
            'module': module,
            'updated_at': last_updated
        }

        db_util.pg_load(conn_id, [dct], 'etl_log.chargebee', fields, constraints=['sitename', 'module'])

    def get_chargebee_config(conn_id):
        query = '''
        SELECT sitename, api_key, module
        FROM etl_conf.chargebee
        WHERE enabled = True
        '''

        return list(db_util.exec_sql(conn_id, query))

    def etl(conn_id):

        logging.info('Getting Chargebee Configuration from Data Warehouse')
        conf = get_chargebee_config(conn_id)

        for elem in conf:
            sitename, api_key, module = elem

            table = f'''w0_chargebee.{sitename}_{module}'''.replace('-', '_')

            logging.info(f'Beginning ETL for {sitename} - {module} into {table}')

            last_sync_date = get_last_sync_date(conn_id, sitename, module)

            logging.info(f'Last Table Sync Time: {last_sync_date}')
            chargebee.configure(api_key, sitename)

            logging.info('Beginning Extract')
            docs = extract(module, last_sync_date=last_sync_date)
            logging.info(f'Extracted {len(docs)} documents')

            if len(docs) == 0:
                logging.info('Skipping...')
                continue

            logging.info('Beginning Transform')
            docs_t = transform(docs)

            logging.info('Beginning Load')
            load(conn_id, docs_t, table)

            logging.info('Updated ETL Log')
            update_etl_log(conn_id, docs, sitename, module)


    load_metadata = PythonOperator(
        task_id='etl',
        python_callable=etl,
        op_kwargs={
            'conn_id': 'postgres_prod',
        }
    )

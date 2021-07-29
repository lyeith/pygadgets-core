#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 12/10/20 4:34 pm

@author: David Wong
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import FacebookAdsInsightsToSqlOperator

from conf import default_args

time_string = '{{ ts_nodash }}'
FACEBOOK_CONN_ID = 'facebook_conn'
ACCOUNT_ID = [295206731534537, 275387770222890, 898359690631290, 502604490299991]
DB_CONN_ID = 'postgres_prod'
DB_SCHEMA = 'w0_external'

default_args['retries'] = 5,
default_args['retry_delay'] = timedelta(minutes=5)

dag_params = {
    'dag_id': 'facebook_ads_to_postgres',
    'default_args': default_args,
    'start_date': datetime(2020, 10, 1, 8, 0, 0),
    'catchup': True,
    'schedule_interval': '@daily',
}

execution_date = '{{ execution_date }}'
next_execution_date = '{{ next_execution_date }}'


breakdowns = [
    {
        'name': 'age_gender',
        'fields': [
            {'name': 'age', 'type': 'varchar(64)'},
            {'name': 'gender', 'type': 'varchar(64)'}
        ]
    },
    {
        'name': 'device_platform',
        'fields': [
            {'name': 'device_platform', 'type': 'varchar(64)'}
        ]
    },
    {
        'name': 'region_country',
        'fields': [
            {'name': 'region', 'type': 'varchar(128)'},
            {'name': 'country', 'type': 'varchar(128)'}
        ]
    },
    {
        'name': 'no_breakdown',
        'fields': []
    }
]

fields = [
    {'name': 'account_id', 'type': 'varchar(64)'},
    {'name': 'ad_id', 'type': 'varchar(64)'},
    {'name': 'adset_id', 'type': 'varchar(64)'},
    {'name': 'campaign_id', 'type': 'varchar(64)'},
    {'name': 'date_start', 'type': 'date'},
    {'name': 'date_stop', 'type': 'date'},
    {'name': 'ad_name', 'type': 'varchar(255)'},
    {'name': 'adset_name', 'type': 'varchar(255)'},
    {'name': 'campaign_name', 'type': 'varchar(255)'},
    {'name': 'clicks', 'type': 'int(11)'},
    {'name': 'cpc', 'type': 'decimal(20,6)'},
    {'name': 'cpm', 'type': 'decimal(20,6)'},
    {'name': 'cpp', 'type': 'decimal(20,6)'},
    {'name': 'ctr', 'type': 'decimal(20,6)'},
    {'name': 'impressions', 'type': 'int(11)'},
    {'name': 'objective', 'type': 'varchar(255)'},
    {'name': 'reach', 'type': 'int(11)'},
    {'name': 'social_spend', 'type': 'decimal(20,6)'},
    {'name': 'spend', 'type': 'decimal(20,6)'},
]

field_names = [field['name'] for field in fields]

# Add any custom fields after building insight api field_names
fields.extend([{'name': 'example', 'type': 'text'}])

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    for breakdown in breakdowns:

        breakdown_fields = [field['name'] for field in breakdown['fields']]

        S3_KEY = 'facebook_insights/{}_{}'.format(breakdown['name'], time_string)

        facebook_ads = FacebookAdsInsightsToSqlOperator(
            task_id='facebook_ads_{}'.format(breakdown['name']),
            facebook_conn_id=FACEBOOK_CONN_ID,
            db_conn_id=DB_CONN_ID,
            db_table=f'''{DB_SCHEMA}.facebook_ads_{breakdown['name']}''',
            account_ids=ACCOUNT_ID,
            insight_fields=field_names,
            breakdowns=breakdown_fields,
            since=execution_date,
            until=next_execution_date,
            time_increment=1,
            level='ad',
            limit=500,
            bootstrap=False,
            dag=dag
        )

        start >> facebook_ads
        facebook_ads >> end

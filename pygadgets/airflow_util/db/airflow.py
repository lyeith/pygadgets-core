#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 17/9/20 12:27 pm

@author: David Wong
"""
import csv
import logging
import os
import psycopg2
import psycopg2.extras
import psycopg2.extensions

from contextlib import closing

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook


def exec_sql(conn_id, query, parameters=None, fetch=True):
    logging.info(f'Executing: {query}')

    hook = BaseHook.get_hook(conn_id)

    if fetch:
        res = hook.get_records(query, parameters=parameters)
    else:
        res = hook.run(query, parameters=parameters)

    if hasattr(hook, 'conn'):
        for output in hook.conn.notices:
            logging.info(output)

    return res


def extract_s3(conn_id, s3_bucket, s3_key):
    logging.info(f'Retrieving: s3://{s3_bucket}/{s3_key}')

    return (S3Hook(conn_id).get_key(s3_key, bucket_name=s3_bucket)
            .get()['Body'].read().decode('utf-8'))
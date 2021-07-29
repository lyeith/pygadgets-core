#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 8/9/20 4:42 pm

@author: David Wong
"""
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import dateutil.parser
import json
import logging

from io import StringIO
import pandas as pd
import psycopg2.extras

import numpy
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


class S3ToPostgresOperator(BaseOperator):
    """

    NOTE: To avoid invalid characters, it is recommended
    to specify the character encoding (e.g {"charset":"utf8"}).

    S3 To MySQL Operator

    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param postgres_conn_id:        The destination redshift connection id.
    :type postgres_conn_id:         string
    :param database:                The destination database name.
    :type database:                 string
    :param table:                   The destination mysql table name.
    :type table:                    string
    :param field_schema:            An array of dicts in the following format:
                                    {'name': 'column_name', 'type': 'int(11)'}
                                    which determine what fields will be created
                                    and inserted.
    :type field_schema:             array
    :param primary_key:             The primary key for the
                                    destination table. Multiple strings in the
                                    array signify a compound key.
    :type primary_key:              array
    :param incremental_key:         *(optional)* The incremental key to compare
                                    new data against the destination table
                                    with. Only required if using a load_type of
                                    "upsert".
    :type incremental_key:          string
    :param load_type:               The method of loading into MySQL that
                                    should occur. Options are "append",
                                    "rebuild", and "upsert". Defaults to
                                    "append."
    :type load_type:                string
    """

    template_fields = ('s3_key',)

    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 postgres_conn_id,
                 schema,
                 table,
                 field_schema,
                 primary_key=[],
                 incremental_key=None,
                 load_type='append',
                 *args,
                 **kwargs):
        super(S3ToPostgresOperator, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.schema = schema
        self.field_schema = field_schema
        self.primary_key = primary_key
        self.incremental_key = incremental_key
        self.load_type = load_type

    def execute(self, context):
        db_hook = PostgresHook(self.postgres_conn_id)

        data = (S3Hook(self.s3_conn_id)
                .get_key(self.s3_key, bucket_name=self.s3_bucket)
                .get()['Body'].read().decode('utf-8'))

        with open(self.table, 'w') as f:
            f.write(data)

        df = pd.read_csv(StringIO(data), sep='\t')
        self.bulk_load(db_hook, df)


    def load(self, db_hook, df):

        schema = {}

        columns = str(tuple(df.columns)).replace("'", "")
        values = [f'''%({e})s''' for e in df.columns]
        values = ', '.join(values)
        query = f'''
        INSERT INTO {self.schema}.{self.table}
        ({columns}),
        VALUES ({values})
        '''

    def bulk_load(self, db_hook, df):

        engine = db_hook.get_sqlalchemy_engine()
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # columns = str(tuple(df.columns)).replace("'", "")
        # values = [f'''%({e})s''' for e in df.columns]
        # values = ', '.join(values)
        #
        # query = f'''
        # INSERT INTO {self.schema}.{self.table} {columns}
        # VALUES ({values})
        # '''
        #
        # print(query)
        # import numpy as np
        # df.replace(np.nan, None, inplace=True)
        # params = df.to_records(dict)
        # psycopg2.extras.execute_batch(cursor, query, params)

        output = StringIO()
        df.to_csv(output, sep='\t', header=False, float_format='%.10g', index=False)

        table = f'{self.schema}.{self.table}'

        output.seek(0)
        cursor.execute(f'''TRUNCATE TABLE {table}''')
        connection.commit()
        cursor.copy_from(output, table, null='', sep='\t')

        connection.commit()
        cursor.close()


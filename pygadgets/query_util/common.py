#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 24/6/20 10:35 pm

@author: David Wong
"""
from pygadgets import db_util


def drop_table(conn, schema, table):
    query = f'''
    DROP TABLE IF EXISTS {schema}.{table}
    '''

    db_util.common.exec_sql(conn, query, fetch=False)


def create_table(conn, schema, table, fields):
    query_builder = []
    for field, prop in fields.items():
        query_builder.append(f'{field} {prop}')

    props = ', '.join(query_builder)

    query = f'''
        CREATE TABLE {schema}.{table} (
        {props} )
    '''

    db_util.common.exec_sql(conn, query, fetch=False)


def pg_load(conn_pg, docs, schema, table, fields):
    field_query, value_query = [], []
    for key, prop in fields.items():
        if 'serial' in prop:
            continue

        value_query.append(f'%({key})s')
        field_query.append(f'{key}')

    value_joined = ', '.join(value_query)
    field_joined = ', '.join(field_query)

    query = f'''
    INSERT INTO {schema}.{table} ({field_joined})
    VALUES ({value_joined})
    '''

    for doc in docs:
        for key in fields.keys():
            if not key in doc:
                doc[key] = None

    db_util.common.pg_insert_many_sql(conn_pg, query, docs)


def main():
    pass


if __name__ == '__main__':
    main()

#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 24/6/20 10:35 pm

@author: David Wong
"""
from pygadgets import db_util, gadgets


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


def pg_load(conn_pg, docs, schema, table, fields, constraints=None):

    def generate_postgres_upsert(table, constraints, variables):

        variables = [e for e in variables if e not in constraints]
        all_fields = constraints + variables

        all_fields_query = ', '.join(all_fields)
        prepared_all_fields_query = ', '.join([f'%({e})s ' for e in all_fields])
        constraints_query = ', '.join(constraints)
        prepared_set_statement = ', '.join([f'{e} = %({e})s ' for e in variables])
        condition_statement = 'AND '.join([f'a.{e} = %({e})s ' for e in constraints])

        query = f'''
        INSERT INTO {table} AS a({all_fields_query})
        VALUES ({prepared_all_fields_query})
        ON CONFLICT ({constraints_query}) DO UPDATE SET
        {prepared_set_statement}
        WHERE {condition_statement};
        '''

        return query

    if constraints:
        keys = [e for e in fields if fields[e] != 'serial']
        query = generate_postgres_upsert(f'{schema}.{table}', constraints, keys)
    else:
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
            if not gadgets.truthy(doc[key]):
                doc[key] = None

    db_util.common.pg_insert_many_sql(conn_pg, query, docs)


def main():
    pass


if __name__ == '__main__':
    main()

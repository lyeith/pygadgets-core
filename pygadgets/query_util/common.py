#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 24/6/20 10:35 pm

@author: David Wong
"""
import csv
import logging
import os

from pygadgets import db_util, gadgets


def generate_ddl(fields, table, constraints=None):

    schema, table_name = tuple(table.split('.'))

    query_builder = [f'"{field}" {prop}' for field, prop in fields.items()]

    if constraints:
        primary_key = ', '.join(constraints)
        query_builder.append(f'CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_key})')

    props = ', '.join(query_builder)
    return f'CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({props})'


def drop_table(conn, table):

    query = f'''DROP TABLE IF EXISTS {table}'''
    db_util.common.exec_sql(conn, query, fetch=False)


def create_table(conn, table, fields, constraints=None):

    ddl = generate_ddl(fields, table, constraints)
    db_util.common.exec_sql(conn, ddl, fetch=False)


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


def generate_bulk_upsert(table, tmp_table, constraints, variables):

    queries = []
    queries.append(f'''LOCK TABLE {table} IN EXCLUSIVE MODE;''')

    if constraints:

        set_variables = ', '.join([f'"{e}" = src."{e}"' for e in variables])
        constraint_query = ' AND '.join([f'src."{e}" = dest.{e}' for e in constraints])

        select_variables = ', '.join([f'src."{e}"' for e in variables])
        null_constraint_query = ' AND '.join([f'dest."{e}" IS NULL' for e in constraints])
        join_using_query = ', '.join(constraints)

        queries.append(f'''
        UPDATE {table} AS dest
        SET {set_variables}
        FROM {tmp_table} AS src
        WHERE {constraint_query};
        ''')

        queries.append(f'''
        INSERT INTO {table}
        SELECT {select_variables}
        FROM {tmp_table} src
        LEFT OUTER JOIN {table} dest 
            USING ({join_using_query})
        WHERE {null_constraint_query};
        ''')

        queries.append('COMMIT;')

    return queries


def copy_expert(docs, table, fields, cur):
    logging.info(f'Creating TSV File')
    filename = f'{table}.tsv'

    try:
        with open(filename, 'w') as f:
            dict_writer = csv.DictWriter(f, fields, delimiter='\t')
            for row in docs:
                # Remove null characters from strings
                for k, v in row.items():
                    if type(v) == str:
                        row[k] = v.replace('\x00', '')
                dict_writer.writerow(row)
    except:
        os.unlink(filename)
        raise

    logging.info('Executing Bulk Load')
    sql = f''' COPY {table} FROM STDIN delimiter '\t' csv NULL AS ''; '''
    if not os.path.isfile(filename):
        with open(filename, 'w'):
            pass

    with open(filename, 'r+') as f:
        cur.copy_expert(sql, f)
        f.truncate(f.tell())
    os.unlink(filename)


def pg_load(conn, docs, table, fields, constraints=None, bulk=False):

    if bulk is True:
        cur = conn.cursor()

        if constraints:

            tmp_table = f'''__{table.split('.')[1]}'''

            logging.info('Creating Temporary Table')
            query = f'''CREATE TEMPORARY TABLE {tmp_table} as (SELECT * FROM {table} limit 0);'''
            cur.execute(query)

            logging.info('Bulk Loading into Temporary Table')
            copy_expert(docs, tmp_table, fields, cur)

            logging.info('Upserting from Temporary Table')
            variables = [e for e in fields if fields[e] != 'serial']
            for query in generate_bulk_upsert(table, tmp_table, constraints, variables):
                cur.execute(query)

        else:
            copy_expert(docs, table, fields, cur)

        conn.commit()

        logging.info('Done')

    else:

        if constraints:
            keys = [e for e in fields if fields[e] != 'serial']
            query = generate_postgres_upsert(f'{table}', constraints, keys)
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
                        INSERT INTO {table} ({field_joined})
                        VALUES ({value_joined})
                        '''

        for doc in docs:
            for key in fields.keys():
                if key not in doc:
                    doc[key] = None
                if not gadgets.truthy(doc[key]):
                    doc[key] = None

        db_util.common.pg_insert_many_sql(conn, query, docs)


def convert_data_types(fields, src_db='mysql', dest_db='postgres'):
    """
    Rough Conversion of Data Types -- Incomplete
    :param fields:
    :param src_db:
    :param dest_db:
    :return:
    """

    data_type_map = {
        'mysql': {
            'postgres': {
                'date': 'date',
                'tinyint': 'smallint',
                'smallint': 'smallint',
                'mediumint': 'integer',
                'int': 'bigint',
                'bigint': 'numeric',
                'float': 'real',
                'double': 'double precision',
                'tinytext': 'varchar',
                'mediumtext': 'varchar',
                'longtext': 'varchar',
                'varchar': 'varchar',
                'text': 'varchar',
                'char': 'char',
                'binary': 'bytea',
                'varbinary': 'bytea',
                'tinyblob': 'bytea',
                'blob': 'bytea',
                'mediumblob': 'bytea',
                'longblob': 'bytea',
                'datetime': 'timestamp',
                'time': 'time',
                'decimal': 'decimal',
                'json': 'jsonb'
            }
        }
    }

    for elem in fields:
        elem['data_type'] = data_type_map[src_db][dest_db][elem['data_type']]

        if elem['data_type'] == 'decimal':
            elem['data_type'] += f'''{int(elem['numeric_precision']), int(elem['numeric_scale'])}'''

    fields = {e['column_name']: e['data_type'] for e in fields}

    return fields


def get_table_fields(conn, table, dialect='mysql'):
    schema, table_name = tuple(table.split('.'))

    if dialect == 'mysql':
        query = f'''
        SELECT column_name, data_type, numeric_precision, numeric_scale, column_key as constraint_type
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table_name}'
        '''
    elif dialect == 'postgres':
        query = f'''
        SELECT c.column_name, c.data_type, c.numeric_precision, c.numeric_scale, constraint_type
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage kcu 
            ON kcu.column_name = c.column_name 
            AND kcu.table_schema = c.table_schema 
            AND kcu.table_name = c.table_name 
        LEFT JOIN information_schema.table_constraints tco
             ON kcu.constraint_name = tco.constraint_name
             AND kcu.constraint_schema = tco.constraint_schema
             AND kcu.constraint_name = tco.constraint_name
        WHERE c.table_schema = '{schema}'
        AND c.table_name = '{table_name}'
        '''

    res = list(db_util.common.exec_sql(conn, query))

    if type(res[0]) == tuple:
        primary_keys = [e[0] for e in res if e[4] in ('PRI', 'PRIMARY KEY')]
    elif type(res[0]) == dict:
        primary_keys = [e['column_name'] for e in res if e['constraint_type'] in ('PRI', 'PRIMARY KEY')]

    return res, primary_keys


def main():
    pass


if __name__ == '__main__':
    main()

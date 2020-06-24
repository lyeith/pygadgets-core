#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:47:08 2019
@author: david
"""
import pymongo
import pymysql
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import time

import datetime


def connect_mongodb(conf, timezone=None, server_timeout=10, socket_timeout=90):
    '''
    :param conf: dictionary (username, password, host, database)
    :return:
    '''

    client_c = pymongo.MongoClient('mongodb://{username}:{password}@{host}/{database}'.format(
        **conf), serverSelectionTimeoutMS=server_timeout * 1000, socketTimeoutMS=socket_timeout * 1000)

    res = client_c.admin.command('ismaster')
    db_time = res['localTime'].replace(tzinfo=datetime.timezone.utc)

    if timezone:
        db_time = db_time.astimezone(tz=timezone)

    return client_c, db_time


def connect_mysql(conf, ssl=None, timezone=None, autocommit=True, timeout=30):
    conv = pymysql.converters.conversions.copy()
    conv[246] = float

    conn_m = pymysql.connect(**conf, ssl=ssl, conv=conv, read_timeout=timeout, autocommit=autocommit)
    exec_sql(conn_m, 'SET @@session.time_zone = \'+07:00\'', fetch=False)

    res = exec_sql(conn_m, 'SELECT NOW()')
    db_time = list(res[0].values())[0]

    if timezone:
        db_time = db_time.replace(tzinfo=timezone)

    return conn_m, db_time


def connect_postgres(conf, timezone=None, readonly=True, autocommit=True, timeout=15):
    conn_pa = psycopg2.connect(**conf, connect_timeout=timeout)

    conn_pa.set_session(readonly=readonly, autocommit=autocommit)
    exec_sql(conn_pa, 'SET TIMEZONE TO \'Asia/Jakarta\'', fetch=False)

    res = exec_sql(conn_pa, 'SELECT NOW()')
    db_time = list(res[0].values())[0]

    if timezone:
        db_time = db_time.replace(tzinfo=timezone)

    return conn_pa, db_time


def exec_sql(conn, query, param=None, dct=True, fetch=True, page=False):

    if type(conn) == pymysql.connections.Connection and dct:
        cur = conn.cursor(pymysql.cursors.SSDictCursor) if page else conn.cursor(pymysql.cursors.DictCursor)
    elif type(conn) == psycopg2.extensions.connection and dct:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    elif type(conn) == sqlite3.Connection and dct:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    else:
        cur = conn.cursor()

    if param:
        cur.execute(query, param)
    else:
        cur.execute(query)

    if fetch and page:
        if type(conn) == pymysql.connections.Connection:
            res = cur.fetchall_unbuffered()
        else:
            res = iter(cur.fetchone, None)
    else:
        res = cur.fetchall() if fetch else []
        res = res if res else []
        cur.close()

    return res


def exec_sql_transaction(conn, query_lst, param_lst=None):
    conn.begin()

    if type(conn) == pymysql.connections.Connection:
        cur = conn.cursor(pymysql.cursors.DictCursor)
    elif type(conn) == psycopg2.extensions.connection:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = conn.cursor()

    try:
        for idx, query in enumerate(query_lst):
            if param_lst:
                cur.execute(query, param_lst[idx])
            else:
                cur.execute(query)
    except:
        conn.rollback()
        raise
    else:
        conn.commit()


def pg_insert_many_sql(conn, query, params):
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, query, params)
    conn.commit()


def upsert_postgres_query(table, constraint, keys, variables):
    sql = list()
    sql.append('INSERT INTO %s AS a(' % table)
    sql.append(', '.join(keys + variables))
    sql.append(') VALUES (')
    sql.append(', '.join(['%({})s '.format(x, x) for x in (keys + variables)]))
    sql.append(') ON CONFLICT ON CONSTRAINT %s DO UPDATE SET ' % constraint)
    sql.append(', '.join(['{} = %({})s '.format(x, x) for x in variables]))
    sql.append('WHERE ')
    sql.append('AND '.join(['a.{} = %({})s '.format(x, x) for x in keys]))
    sql.append(';')

    return ''.join(sql)
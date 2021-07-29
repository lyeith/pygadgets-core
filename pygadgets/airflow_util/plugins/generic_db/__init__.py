#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 29/7/20 5:22 pm

@author: David Wong
"""
from airflow.plugins_manager import AirflowPlugin
from plugins.generic_db.operators.sql import SqlOperator
from plugins.generic_db.operators.sql_to_sql import SqlToSql

class MySqlToPostgresPlugin(AirflowPlugin):

    name = 'generic_db'

    operators = [SqlOperator, SqlToSql]

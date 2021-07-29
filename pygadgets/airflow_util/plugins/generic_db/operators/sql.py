#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 29/7/20 5:11 pm

@author: David Wong
"""
from typing import List, Optional, Union

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SqlOperator(BaseOperator):
    """
    Moves data from a connection to another, assuming that they both
    provide the required methods in their respective hooks. The source hook
    needs to expose a `get_records` method, and the destination a
    `insert_rows` method.
    This is meant to be used on small-ish datasets that fit in memory.
    :param sql: SQL query to execute against the source database. (templated)
    :type sql: str
    :param conn_id: source connection
    :type conn_id: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql', '.hql',)
    # ui_color = '#b0f07c'

    @apply_defaults
    def __init__(
            self, sql: str,
            conn_id: str, autocommit: bool,
            parameters: list = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = BaseHook.get_hook(self.conn_id)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)

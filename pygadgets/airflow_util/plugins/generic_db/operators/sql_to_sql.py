#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import csv
import pendulum
import os

from typing import List, Optional, Union

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from util import db_util


class SqlToSql(BaseOperator):
    template_fields = ('exec_date', 'prev_exec_date')
    ui_color = '#b0f07c'

    @apply_defaults
    def __init__(
            self,
            *,
            source_table: str,
            destination_table: str,
            source_conn_id: str,
            destination_conn_id: str,
            exec_date: pendulum,
            prev_exec_date: pendulum,
            filters: Optional[List[dict]] = {},
            preoperator: Optional[Union[str, List[str]]] = None,
            insert_dialect: str = 'generic',
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.source_table = source_table
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.exec_date = exec_date
        self.prev_exec_date = prev_exec_date
        self.filters = filters
        self.preoperator = preoperator

        self.insert_dialect = insert_dialect

    def execute(self, context):
        t_func = {
            'timestamp': lambda val: pendulum.parse(val).timestamp(),
            'datetime': lambda val: pendulum.parse(val).to_datetime_string()
        }

        source_hook = BaseHook.get_hook(self.source_conn_id)
        destination_hook = BaseHook.get_hook(self.destination_conn_id)

        fields, constraints = db_util.get_table_fields(self.source_conn_id, self.source_table)
        fields = db_util.convert_data_types(fields, src_db='mysql', dest_db='postgres')
        db_util.create_table(self.destination_conn_id, self.destination_table, fields, constraints)

        condition_lst, conditions = [], ''

        for filter, data_type in self.filters.items():
            date_start = t_func[data_type](self.prev_exec_date)
            date_end = t_func[data_type](self.exec_date)

            if type(date_start) == str:
                condition_lst.append(f'''
                ({filter} > '{date_start}' AND 
                {filter} <= '{date_end}')
                ''')
            else:
                condition_lst.append(f'''
                ({filter} > {date_start} AND 
                {filter} <= {date_end})
                ''')

        if condition_lst:
            conditions = 'OR '.join(condition_lst)
            conditions = f'WHERE {conditions}'

        sql = f'''
        SELECT *
        FROM {self.source_table}
        {conditions}
        '''

        self.log.info(f'Extracting data from {self.source_conn_id}')
        self.log.info(f'Executing: \n {sql}')

        cur = source_hook.get_cursor()
        cur.execute(sql)

        if self.preoperator:
            self.log.info('Running preoperator')
            self.log.info(self.preoperator)
            destination_hook.run(self.preoperator, autocommit=True)

        self.log.info(f'Inserting rows into {self.destination_conn_id}')

        if self.insert_dialect == 'postgres':

            if self.filters:
                db_util.pg_bulk_load(self.destination_conn_id, cur, self.destination_table, fields, constraints)
            else:
                db_util.pg_bulk_load(self.destination_conn_id, cur, self.destination_table, fields, constraints=None)

            # if self.filters:
            #     self.log.info('Executing Upsert Operation')
            #     db_util.pg_load(self.destination_conn_id, cur, self.destination_table, fields, constraints)
            #
            # elif self.bulk_load:
            #     db_util.pg_bulk_load(self.destination_conn_id, cur, self.destination_table, fields, constraints)
            # else:
            #     self.log.info('Executing Normal Load Operation')
            #     db_util.pg_load(self.destination_conn_id, cur, self.destination_table, fields)
        else:
            self.log.info('Executing Normal Load Operation')
            destination_hook.insert_rows(table=self.destination_table, rows=cur)

import json
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook

from util import db_util


class GoogleAnalyticsReportingToSqlOperator(BaseOperator):
    """
    Google Analytics Reporting To S3 Operator

    :param google_analytics_conn_id:    The Google Analytics connection id.
    :type google_analytics_conn_id:     string
    :param view_id:                     The view id for associated report.
    :type view_id:                      string/array
    :param since:                       The date up from which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type since:                        string
    :param until:                       The date up to which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type until:                        string
    :param db_conn_id:                  The db connection id.
    :type db_conn_id:                   string
    """

    template_fields = ('since',
                       'until')

    def __init__(self,
                 google_analytics_conn_id,
                 view_id,
                 since,
                 until,
                 dimensions,
                 metrics,
                 db_conn_id,
                 db_table,
                 page_size=1000,
                 include_empty_rows=True,
                 sampling_level=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_analytics_conn_id = google_analytics_conn_id
        self.view_id = view_id
        self.since = since
        self.until = until
        self.sampling_level = sampling_level
        self.dimensions = dimensions
        self.metrics = metrics
        self.page_size = page_size
        self.include_empty_rows = include_empty_rows
        self.db_conn_id = db_conn_id
        self.db_table = db_table

        self.metricMap = {
            'METRIC_TYPE_UNSPECIFIED': 'varchar(255)',
            'CURRENCY': 'decimal(20,5)',
            'INTEGER': 'int(11)',
            'FLOAT': 'decimal(20,5)',
            'PERCENT': 'decimal(20,5)',
            'TIME': 'time'
        }

        if self.page_size > 10000:
            raise Exception('Please specify a page size equal to or lower than 10000.')

        if not isinstance(self.include_empty_rows, bool):
            raise Exception('Please specificy "include_empty_rows" as a boolean.')

    def execute(self, context):

        def extract(google_analytics_conn_id):
            ga_conn = GoogleAnalyticsHook(google_analytics_conn_id)

            try:
                since_formatted = datetime.strptime(self.since, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                since_formatted = str(self.since)
            try:
                until_formatted = datetime.strptime(self.until, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                until_formatted = str(self.until)
            report = ga_conn.get_analytics_report(self.view_id,
                                                  since_formatted,
                                                  until_formatted,
                                                  self.sampling_level,
                                                  self.dimensions,
                                                  self.metrics,
                                                  self.page_size,
                                                  self.include_empty_rows)

            columnHeader = report.get('columnHeader', {})
            # Right now all dimensions are hardcoded to varchar(255), will need a map if any non-varchar dimensions are used in the future
            # Unfortunately the API does not send back types for Dimensions like it does for Metrics (yet..)
            dimensionHeaders = [
                {'name': header.replace('ga:', ''), 'type': 'varchar(255)'}
                for header
                in columnHeader.get('dimensions', [])
            ]
            metricHeaders = [
                {'name': entry.get('name').replace('ga:', ''),
                 'type': self.metricMap.get(entry.get('type'), 'varchar(255)')}
                for entry
                in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            ]

            lst = []
            rows = report.get('data', {}).get('rows', [])

            for row_counter, row in enumerate(rows):
                root_data_obj = {}
                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])

                for index, dimension in enumerate(dimensions):
                    header = dimensionHeaders[index].get('name').lower()
                    root_data_obj[header] = dimension

                for metric in metrics:
                    data = {}
                    data.update(root_data_obj)

                    for index, value in enumerate(metric.get('values', [])):
                        header = metricHeaders[index].get('name').lower()
                        data[header] = value

                    data['viewid'] = self.view_id
                    data['timestamp'] = self.since

                    lst.append(data)

            return lst

        def transform(docs):
            def strip_not_set(val):
                return val if val not in ['(not set)', '(not provided)', '(none)'] else None

            t_func = {
                'datetime': lambda val: datetime.strptime(val, '%Y%m%d%H'),
                'keyword': strip_not_set,
                'referralpath': strip_not_set,
                'campaign': strip_not_set,
                'source': strip_not_set,
                'medium': strip_not_set,
                'pageviews': lambda val: int(val),
                'bounces': lambda val: int(val),
                'users': lambda val: int(val),
                'newusers': lambda val: int(val),
                'adclicks': lambda val: int(val),
                'cpm' : lambda val: float(val),
                'cpc' : lambda val: float(val),
                'ctr' : lambda val: float(val),
                'avgsessionduration' : lambda val: float(val),
            }

            for doc in docs:

                doc['datetime'] = doc.pop('date') + doc.pop('hour')
                doc['source'], doc['medium'] = doc.pop('sourcemedium').split(' / ')

                for key, func in t_func.items():
                    doc[key] = func(doc[key])

            return docs

        def load(conn_id, docs, table):
            fields, constraints = db_util.get_table_fields(conn_id, table, dialect='postgres')
            fields = {e[0]: e[1] for e in fields}
            db_util.pg_load(conn_id, docs, table, fields, constraints)


        docs = extract(self.google_analytics_conn_id)
        docs = transform(docs)
        load(self.db_conn_id, docs, self.db_table)

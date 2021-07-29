from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from google_analytics_plugin.schemas.google_analytics_schemas import google_analytics_reporting_schema
from airflow.operators import GoogleAnalyticsReportingToSqlOperator

from conf import default_args

GOOGLE_ANALYTICS_CONN_ID = 'ga_main'

# Google Analytics has a "lookback window" that defaults to 30 days.
# During this period, metrics are in flux as users return to the property
# and complete various actions and conversion goals.
# https://support.google.com/analytics/answer/1665189?hl=en

# The period set as the LOOKBACK_WINDOW will be dropped and replaced during
# each run of this workflow.

LOOKBACK_WINDOW = 30

# NOTE: While GA supports relative input dates, it is not advisable to use
# these in case older workflows need to be re-run.

# https://developers.google.com/analytics/devguides/reporting/core/v4/basics
SINCE = "{{{{ macros.ds_add(ds, -{0}) }}}}".format(str(LOOKBACK_WINDOW))
UNTIL = "{{ ds }}"

view_ids = [106710411,
            178683781,
            226956185,
            214892222,
            224195176,
            171699580]

# https://developers.google.com/analytics/devguides/reporting/core/v3/reference#sampling
SAMPLING_LEVEL = None

# https://developers.google.com/analytics/devguides/reporting/core/v3/reference#includeEmptyRows
INCLUDE_EMPTY_ROWS = False

PAGE_SIZE = 1000

# NOTE: Not all metrics and dimensions are available together. It is
# advisable to test with the GA explorer before deploying.
# https://developers.google.com/analytics/devguides/reporting/core/dimsmets

METRICS = [{'expression': 'ga:pageViews'},
           {'expression': 'ga:bounces'},
           {'expression': 'ga:users'},
           {'expression': 'ga:newUsers'},
           {'expression': 'ga:adClicks'},
           {'expression': 'ga:CPM'},
           {'expression': 'ga:CPC'},
           {'expression': 'ga:CTR'},
           {'expression': 'ga:avgSessionDuration'}]


DIMENSIONS = [{'name': 'ga:date'},
              {'name': 'ga:hour'},
              {'name': 'ga:keyword'},
              {'name': 'ga:referralPath'},
              {'name': 'ga:campaign'},
              {'name': 'ga:sourceMedium'},
              {'name': 'ga:deviceCategory'}]

# The specified TIMEFORMAT is based on the ga:dateHourMinute dimension.
# If using ga:date or ga:dateHour, this format will need to adjust accordingly.
COPY_PARAMS = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TIMEFORMAT 'YYYYMMDDHHMI'"
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

# Primary and Incremental Keys are set to same value as no other reliable
# primary_key can found. This will result in all records with matching values of
# dateHourMinute to be deleted and new records inserted for the period of time
# covered by the lookback window. Timestamps matching records greater than
# the lookback window from the current data will not be pulled again and
# therefore not replaced.

PRIMARY_KEY = 'datehourminute'
INCREMENTAL_KEY = 'datehourminute'

default_args['retries'] = 2,
default_args['retry_delay'] = timedelta(minutes=5)

dag_params = {
    'dag_id': f'{GOOGLE_ANALYTICS_CONN_ID}_to_postgres_hourly',
    'default_args': default_args,
    'start_date': datetime(2020, 10, 1, 8, 0, 0),
    'catchup': False,
    'schedule_interval': '@hourly'
}

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    for view_id in view_ids:

        view_id = str(view_id)
        extract_ga = GoogleAnalyticsReportingToSqlOperator(task_id=f'etl_{view_id}',
                                                 google_analytics_conn_id=GOOGLE_ANALYTICS_CONN_ID,
                                                 view_id=view_id,
                                                 since=SINCE,
                                                 until=UNTIL,
                                                 sampling_level=SAMPLING_LEVEL,
                                                 dimensions=DIMENSIONS,
                                                 metrics=METRICS,
                                                 page_size=PAGE_SIZE,
                                                 include_empty_rows=INCLUDE_EMPTY_ROWS,
                                                 db_conn_id='postgres_prod',
                                                 db_table='w0_external.google_analytics'
                                                 )

        start >> extract_ga
        extract_ga >> end
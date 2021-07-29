from datetime import datetime

from airflow.models import BaseOperator

from airflow.hooks.postgres_hook import PostgresHook
from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook

import pendulum
from util import db_util, transforms


class FacebookAdsInsightsToSqlOperator(BaseOperator):
    """
    Facebook Ads Insights To S3 Operator
    :param facebook_conn_id:        The source facebook connection id.
    :type s3_conn_id:               string
    :param s3_conn_id:              The destination s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param account_ids:             An array of Facebook Ad Account Ids strings which
                                    own campaigns, ad_sets, and ads.
    :type account_ids:              array
    :param insight_fields:          An array of insight field strings to get back from
                                    the API.  Defaults to an empty array.
    :type insight_fields:           array
    :param breakdowns:              An array of breakdown strings for which to group insights.abs
                                    Defaults to an empty array.
    :type breakdowns:               array
    :param since:                   A datetime representing the start time to get Facebook data.
                                    Can use Airflow template for execution_date
    :type since:                    datetime
    :param until:                   A datetime representing the end time to get Facebook data.
                                    Can use Airflow template for next_execution_date
    :type until:                    datetime
    :param time_increment:          A string representing the time increment for which to get data,
                                    described by the Facebook Ads API. Defaults to 'all_days'.
    :type time_increment:           string
    :param level:                   A string representing the level for which to get Facebook Ads data,
                                    can be campaign, ad_set, or ad level.  Defaults to 'ad'.
    :type level:                    string
    :param limit:                   The number of records to fetch in each request. Defaults to 100.
    :type limit:                    integer
    """

    template_fields = ('since', 'until')

    def execute(self, context):

        def extract(facebook_conn_id):
            facebook_conn = FacebookAdsHook(facebook_conn_id)

            time_range = {
                'since': pendulum.parse(self.since).to_date_string(),
                'until': pendulum.parse(self.until).to_date_string()
            }

            lst = []
            for account_id in self.account_ids:

                account_id = str(account_id)
                insights = facebook_conn.get_insights_for_account_id(account_id,
                                                                     self.insight_fields,
                                                                     self.breakdowns,
                                                                     time_range,
                                                                     self.time_increment,
                                                                     self.level,
                                                                     self.limit,
                                                                     self.bootstrap)

                if len(insights) > 0:
                    lst += insights

            return lst

        def transform(docs):

            t_func = {
                'datetime': lambda val: datetime.strptime(val, '%Y-%m-%d'),
                'clicks': transforms.to_int,
                'cpc': transforms.to_float,
                'cpm' : transforms.to_float,
                'cpp' : transforms.to_float,
                'ctr' : transforms.to_float,
                'impressions' : transforms.to_int,
                'reach': transforms.to_int,
                'social_spend': transforms.to_float,
                'spend': transforms.to_float,
            }

            for doc in docs:

                doc['datetime'] = doc.pop('date_start')

                if self.breakdowns:
                    doc['dimension'] = doc.pop(self.breakdowns[0])

                for key, func in t_func.items():
                    doc[key] = func(doc.get(key))

            return docs

        def load(conn_id, docs, table):
            fields, constraints = db_util.get_table_fields(conn_id, table, dialect='postgres')
            fields = {e[0]: e[1] for e in fields}
            db_util.pg_load(conn_id, docs, table, fields, constraints)

        docs = extract(self.facebook_conn_id)
        docs = transform(docs)
        load(self.db_conn_id, docs, self.db_table)

    def __init__(self,
                 facebook_conn_id,
                 db_conn_id,
                 db_table,
                 account_ids,
                 insight_fields,
                 breakdowns,
                 since,
                 until,
                 time_increment='all_days',
                 level='ad',
                 limit=500,
                 bootstrap=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.facebook_conn_id = facebook_conn_id
        self.db_conn_id = db_conn_id
        self.db_table = db_table
        self.account_ids = account_ids
        self.insight_fields = insight_fields
        self.breakdowns = breakdowns
        self.since = since
        self.until = until
        self.time_increment = time_increment
        self.level = level
        self.limit = limit
        self.bootstrap = bootstrap

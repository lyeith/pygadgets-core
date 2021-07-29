import json
from datetime import datetime

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook


class FacebookAdsInsightsToS3Operator(BaseOperator):
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

    template_fields = ('s3_key', 'since', 'until')

    def __init__(self,
                 facebook_conn_id,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 account_ids,
                 insight_fields,
                 breakdowns,
                 since,
                 until,
                 time_increment='all_days',
                 level='ad',
                 limit=100,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.facebook_conn_id = facebook_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.account_ids = account_ids
        self.insight_fields = insight_fields
        self.breakdowns = breakdowns
        self.since = since
        self.until = until
        self.time_increment = time_increment
        self.level = level
        self.limit = limit

    def execute(self, context):
        facebook_conn = FacebookAdsHook(self.facebook_conn_id)
        s3 = S3Hook(self.s3_conn_id)

        time_range = {
            'since': datetime.strptime(self.since, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d'),
            'until': datetime.strptime(self.until, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d')
        }

        lst = []
        for account_id in self.account_ids:
            insights = facebook_conn.get_insights_for_account_id(account_id,
                                                                 self.insight_fields,
                                                                 self.breakdowns,
                                                                 time_range,
                                                                 self.time_increment,
                                                                 self.level,
                                                                 self.limit)

            if len(insights) > 0:
                lst += insights

        s3.load_string(
            string_data=json.dumps(lst),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

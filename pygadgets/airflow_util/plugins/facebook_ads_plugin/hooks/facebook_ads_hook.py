from urllib.parse import urlencode
import requests
import time

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun

from airflow.hooks.base_hook import BaseHook


class FacebookAdsHook(BaseHook):
    def __init__(self, facebook_ads_conn_id='facebook_ads_default'):
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.connection = self.get_connection(facebook_ads_conn_id)

        self.base_uri = 'https://graph.facebook.com'
        self.access_token = self.connection.extra_dejson.get('accessToken') or self.connection.password
        self.app_id = self.connection.extra_dejson.get('appId')
        self.app_secret = self.connection.extra_dejson.get('appSecret')

    def get_insights_for_account_id(self, 
                                    account_id, 
                                    insight_fields, 
                                    breakdowns, 
                                    time_range, 
                                    time_increment='all_days', 
                                    level='ad', 
                                    limit=100,
                                    bootstrap=False):

        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)

        account = AdAccount(f'act_{account_id}')

        params = {
            'breakdowns': breakdowns,
            'time_range': time_range,
            'date_preset': 'lifetime',
            'time_increment': time_increment,
            'level': level
        }

        if bootstrap:
            params.pop('time_range')
            params['date_preset'] = 'lifetime'

        self.log.info(f'Retrieving Insights for Facebook Ad Account {account_id}')
        async_job = account.get_insights(params=params, fields=insight_fields, is_async=True)

        while True:
            job = async_job.api_get()
            status = job[AdReportRun.Field.async_status]
            self.log.info(f'Status: {status}, Percent done: {str(job[AdReportRun.Field.async_percent_completion])}')
            time.sleep(1)
            if status == "Job Completed":
                self.log.info('Done!')
                break

        return [dict(e) for e in async_job.get_result(params={'limit': limit})]

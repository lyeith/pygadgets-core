from airflow.plugins_manager import AirflowPlugin
from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook
from facebook_ads_plugin.operators.facebook_ads_to_s3_operator import FacebookAdsInsightsToS3Operator
from facebook_ads_plugin.operators.facebook_ads_to_sql_operator import FacebookAdsInsightsToSqlOperator


class FacebookAdsPlugin(AirflowPlugin):
    name = "FacebookAdsPlugin"
    hooks = [FacebookAdsHook]
    operators = [FacebookAdsInsightsToS3Operator, FacebookAdsInsightsToSqlOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

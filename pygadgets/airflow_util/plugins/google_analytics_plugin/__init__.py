from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook
from google_analytics_plugin.operators.google_analytics_reporting_to_s3_operator import GoogleAnalyticsReportingToS3Operator
from google_analytics_plugin.operators.google_analytics_reporting_to_sql_operator import GoogleAnalyticsReportingToSqlOperator
from google_analytics_plugin.operators.google_analytics_account_summaries_to_s3_operator import GoogleAnalyticsAccountSummariesToS3Operator
from google_analytics_plugin.operators.google_analytics_account_summaries_to_sql_operator import GoogleAnalyticsAccountSummariesToSqlOperator


class GoogleAnalyticsPlugin(AirflowPlugin):
    name = "google_analytics_plugin"
    hooks = [GoogleAnalyticsHook]
    operators = [GoogleAnalyticsReportingToS3Operator,
                 GoogleAnalyticsReportingToSqlOperator,
                 GoogleAnalyticsAccountSummariesToS3Operator,
                 GoogleAnalyticsAccountSummariesToSqlOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

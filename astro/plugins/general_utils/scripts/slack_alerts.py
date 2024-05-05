# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.models import Variable
# import logging
# import re
# from pprint import pformat
#
# LOCAL_ENV = 'localhost'
# logger = logging.getLogger()
#
#
# def build_message(context):
#     logger.info(f"Context: {pformat(context)}")
#     log_url = context['task_instance'].log_url
#     is_local = bool(re.search('localhost\\:', log_url))
#     # logger.info(f"Log_Url: {log_url}. Are we local: {is_local}")
#
#
#     msg = f"*Environment*: {'local-dev' if is_local else LOCAL_ENV}\n"
#     msg += f"*Dag*: {context['dag'].dag_id}\n"
#     msg += f"*Task*: {context['task_instance'].task_id}\n"
#     msg += f"*Execution Time*: {context['logical_date']}\n"
#     if 'exception' in context:
#         msg += f"*Exception*: {context['exception']}\n"
#
#     msg += f"*DAG Logs*: <{log_url}|Logs>\n"
#     return msg
#
#
# def slack_alerts(context, slack_conn='slack_alerts_default', level='Error', message=None):
#     """
#     Adapted from https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
#     Parameters:
#         context (DAG Context) (Required) - will get context
#         slack_conn (str) (Optional) - Airflow connection ID for Slack Alerts
#         level (str) (Optional) - level of message ex Error, Warning, Info, Testing
#         message (str) (Optional) - message passed from task
#
#     Returns:
#         Slack message to a channel
#     """
#
#     if message is None and 'exception' in context:
#         exception_str = str(context['exception'])
#         if bool(re.search('^DUMPSTERFIRE.*', exception_str)):
#             logger.info(f'Original Exception Line: {exception_str}')
#             level = 'DUMPSTERFIRE'
#             regex_fix = re.sub('^DUMPSTERFIRE: ', "", exception_str)
#             logger.info(f'Regex Fix: {regex_fix}')
#             context['exception'] = Exception(regex_fix)
#
#     if level.upper() == 'DUMPSTERFIRE':
#         slack_msg = f":party_dumpster_fire: *We have a real issue...* :party_dumpster_fire: \n"
#     elif level.upper() == 'WARNING':
#         slack_msg = f":warning: *Task Warning* :warning:\n"
#     elif level.upper() == 'INFO':
#         slack_msg = f":tinfoil: *Task Info* :tinfoil:\n"
#     elif level.upper() == 'TESTING':
#         slack_msg = f":+1: *Task Info - Testing* :+1:\n"
#     else:
#         slack_msg = f":rotating_light: *Task Failed*:rotating_light:\n"
#
#     slack_msg += build_message(context)
#     if level.upper() == 'TESTING':
#         message = f"This is a test message, this can be ignored. {message if message else ''}"
#
#     if message is not None:
#         slack_msg += f"*Message*: {message}"
#
#     slack_alert = SlackWebhookOperator(
#         task_id='slack_alert',
#         message=slack_msg,
#         http_conn_id=slack_conn
#     )
#
#     try:
#         return slack_alert.execute(context=context)
#     except:
#         slack_alerts(context, message=f'The Slack Connection "{slack_conn}" that was requested does not work/exist. Please check configs')
#

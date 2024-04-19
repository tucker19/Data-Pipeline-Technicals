from datetime import datetime, timedelta

default_args = {
    # Use the same owner and email settings for all operators
    'owner': 'Adam Howell',
    'email': ['adam.dhowell.19@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,

    # These apply to all operators although sensors may override these settings
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'catchup': False
}
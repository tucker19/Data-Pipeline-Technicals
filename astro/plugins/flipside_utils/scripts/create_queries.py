"""
Create table queries for Flipside API queries
"""
CREATE_QUERIES = {
    'Top 10 Senders by amount in last hour': {
        'destination_table':
        'top_10_senders_amount_hour',
        'query':
        """
                   create table if not exists top_10_senders_amount_hour (
                     day_hour_ts timestamp, 
                     sender text, 
                     total_transferred numeric 
                   );
                   """
    },
    'Top 10 Receiver by amount in last hour': {
        'destination_table':
        'top_10_receivers_amount_hour',
        'query':
        """
                   create table if not exists top_10_receivers_amount_hour (
                     day_hour_ts timestamp, 
                     receiver text, 
                     total_transferred numeric
                   );
                   """
    },
    'Top 10 Senders for a block_id by transfers': {
        'destination_table':
        'top_10_senders_by_block_id_day',
        'query':
        """
                   create table if not exists top_10_senders_by_block_id_day (
                     block_id numeric, 
                     sender text, 
                     transfers_per_block numeric, 
                     total_amount numeric
                   );
                   """
    },
    'Top 10 Receivers for a block_id by transfers': {
        'destination_table':
        'top_10_receivers_by_block_id_day',
        'query':
        """
                   create table if not exists top_10_receivers_by_block_id_day (
                     block_id numeric, 
                     receiver text, 
                     transfers_per_block numeric, 
                     total_amount numeric
                   );
                   """
    },
    'Number of Transactions per Day over last week': {
        'destination_table':
        'transactions_per_day',
        'query':
        """
                   create table if not exists transactions_per_day (
                     day_ts timestamp, 
                     total_transactions numeric, 
                     total_amount numeric
                   );
                   """
    },
    'Number of Transactions per Month, last 6 month(s)': {
        'destination_table':
        'transactions_per_month',
        'query':
        """
                   create table if not exists transactions_per_month (
                     month_ts timestamp,
                     month_transfer_total numeric, 
                     month_total_amount numeric, 
                     average_transfers_per_day numeric, 
                     average_amount_per_transfer numeric,
                     max_daily_amaount numeric
                   );
                   """
    }
}

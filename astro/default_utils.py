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

FLIPSIDE_QUERIES = [{
    'name':
    'Top 10 Senders by amount in last hour',
    'query':
    """
    -- Top 10 Senders by amount in last hour
    with sender_totals_per_hour as (
      SELECT DATE_TRUNC('hour', BLOCK_TIMESTAMP) AS day_hour, tx_from AS sender, SUM(amount) AS total_transferred
      FROM solana.core.fact_transfers
      where date_trunc('hour', BLOCK_TIMESTAMP) = date_trunc('hour', (CURRENT_TIMESTAMP() - INTERVAL '1 hour'))
      GROUP BY day_hour, sender),

    senders_ranked as (
    select day_hour, sender, total_transferred, RANK() OVER (PARTITION BY day_hour ORDER BY total_transferred DESC) AS per_hour_transfer_rank
    from sender_totals_per_hour)

    select day_hour, sender, total_transferred
    from senders_ranked
    where per_hour_transfer_rank <= 10
    order by day_hour, total_transferred DESC;
    """
}, {
    'name':
    'Top 10 Receiver by amount in last hour',
    'query':
    """
  -- Top 10 Receiver by amount in last hour
  with receiver_totals_per_hour as (
    SELECT DATE_TRUNC('hour', BLOCK_TIMESTAMP) AS day_hour, TX_TO AS receiver, SUM(amount) AS total_transferred
    FROM solana.core.fact_transfers
    where date_trunc('hour', BLOCK_TIMESTAMP) = date_trunc('hour', (CURRENT_TIMESTAMP() - INTERVAL '1 hour'))
    GROUP BY day_hour, receiver),

  receivers_ranked as (
  select day_hour, receiver, total_transferred, RANK() OVER (PARTITION BY day_hour ORDER BY total_transferred DESC) AS per_hour_transfer_rank
  from receiver_totals_per_hour)

  select day_hour, receiver, total_transferred
  from receivers_ranked
  where per_hour_transfer_rank <= 10
  order by day_hour, total_transferred DESC;
  """
}, {
    'name':
    'Top 10 Senders for a block_id by transfers',
    'query':
    """
    -- Top 10 Senders for a block_id by transfers
    with sender_totals_per_day as (
      SELECT block_id, tx_from AS sender, count(*) as transfers_per_block, sum(amount) as total_amount
      FROM solana.core.fact_transfers
      where date_trunc('day', BLOCK_TIMESTAMP) >= (CURRENT_DATE() - INTERVAL '1 day')
      GROUP BY block_id, sender),

    senders_ranked as (
    select block_id, sender, transfers_per_block, total_amount, RANK() OVER (PARTITION BY block_id ORDER BY transfers_per_block DESC, total_amount DESC) AS per_block_transfer_rank
    from sender_totals_per_day)

    select block_id, sender, transfers_per_block, total_amount
    from senders_ranked
    where per_block_transfer_rank <= 10
    order by block_id, per_block_transfer_rank;
    """
}, {
    'name':
    'Top 10 Receivers for a block_id by transfers',
    'query':
    """
   -- Top 10 Receivers for a block_id by transfers
   with receiver_totals_per_day as (
     SELECT block_id, tx_to AS receiver, count(*) as transfers_per_block, sum(amount) as total_amount
     FROM solana.core.fact_transfers
     where date_trunc('day', BLOCK_TIMESTAMP) >= (CURRENT_DATE() - INTERVAL '1 day')
     GROUP BY block_id, receiver),

   receivers_ranked as (
   select block_id, receiver, transfers_per_block, total_amount, RANK() OVER (PARTITION BY block_id ORDER BY transfers_per_block DESC, total_amount DESC) AS per_block_transfer_rank
   from receiver_totals_per_day)

   select block_id, receiver, transfers_per_block, total_amount
   from receivers_ranked
   where per_block_transfer_rank <= 10
   order by block_id, per_block_transfer_rank;
   """
}, {
    'name':
    'Number of Transactions per Day over last week',
    'query':
    """
   -- Number of Transactions per Day over last week
   select DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day, count(*) as total_transactions, sum(amount) as total_amount
   from solana.core.fact_transfers
   where date_trunc('hour', BLOCK_TIMESTAMP) between (CURRENT_DATE() - INTERVAL '7 day') and (CURRENT_DATE() - INTERVAL '1 day')
   group by day
   order by day;
   """
}#, {
#     'name':
#     'Number of Transactions per Month, last 6 month(s)',
#     'query':
#     """
#    -- Number of Transactions per Month, last 6 month(s)
#    with month_day_trunc as (
#      select DATE_TRUNC('month', BLOCK_TIMESTAMP) AS month, DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day, 1 as transfer_count, amount, row_number() over (PARTITION BY month ORDER BY day) as row_num
#      from solana.core.fact_transfers
#      where date_trunc('month', BLOCK_TIMESTAMP) -- = date_trunc('month',(CURRENT_DATE() - INTERVAL '1 month'))
#      between (CURRENT_DATE() - INTERVAL '6 month') and (CURRENT_DATE() - INTERVAL '1 month')
#    ),

#    month_day_totals as (
#      select month, day, sum(transfer_count) as daily_transfer_total, sum(amount) as daily_total_amount
#      from month_day_trunc
#      group by month, day
#    )

#    select month, -- count(DISTINCT day) as num_of_days,
#      sum(daily_transfer_total) as month_transfer_total,
#      sum(daily_total_amount) as month_total_amount, avg(daily_transfer_total) as average_transfers_per_day,
#      avg(daily_total_amount) as average_amount_per_transfer,
#      min(daily_transfer_total) as min_daily_transfers, max(daily_transfer_total) as max_daily_transfers,
#      min(daily_total_amount) as min_daily_amount, max(daily_total_amount) as max_daily_amaount
#    from month_day_totals
#    group by month
#    order by month;
#    """
# }
]

FLIPSIDE_QUERY_FILES = [
    'sql/Top 10 Senders by amount in last hour.sql',
    'sql/Top 10 Receivers by amount in last hour.sql',
    'sql/Top 10 Senders for a block_id by transfers.sql',
    'sql/Top 10 Receivers for a block_id by transfers.sql',
    'sql/Number of Transactions per Day over last week.sql'#,
    #'sql/Number of Transactions per Month last 6 month.sql'
]

FLIPSIDE_CREATE_QUERIES = []
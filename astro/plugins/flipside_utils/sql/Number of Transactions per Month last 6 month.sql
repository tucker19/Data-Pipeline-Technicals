-- Number of Transactions per Month, last 6 month(s)
with month_day_trunc as (
  select DATE_TRUNC('month', BLOCK_TIMESTAMP) AS month, DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day, 1 as transfer_count, amount, row_number() over (PARTITION BY month ORDER BY day) as row_num
  from solana.core.fact_transfers
  where date_trunc('month', BLOCK_TIMESTAMP) = date_trunc('month',(CURRENT_DATE() - INTERVAL '1 month'))
  -- between (CURRENT_DATE() - INTERVAL '6 month') and (CURRENT_DATE() - INTERVAL '1 month')
),

month_day_totals as (
  select month, day, sum(transfer_count) as daily_transfer_total, sum(amount) as daily_total_amount
  from month_day_trunc
  group by month, day
)

select month, -- count(DISTINCT day) as num_of_days, 
  sum(daily_transfer_total) as month_transfer_total, 
  sum(daily_total_amount) as month_total_amount, avg(daily_transfer_total) as average_transfers_per_day, 
  avg(daily_total_amount) as average_amount_per_transfer,
  min(daily_transfer_total) as min_daily_transfers, max(daily_transfer_total) as max_daily_transfers,
  min(daily_total_amount) as min_daily_amount, max(daily_total_amount) as max_daily_amaount
from month_day_totals
group by month
order by month;
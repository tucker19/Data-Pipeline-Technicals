-- Number of Transactions per Day over last week
select DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day, count(*) as total_transactions, sum(amount) as total_amount
from solana.core.fact_transfers
where date_trunc('hour', BLOCK_TIMESTAMP) between (CURRENT_DATE() - INTERVAL '7 day') and (CURRENT_DATE() - INTERVAL '1 day')
group by day
order by day;
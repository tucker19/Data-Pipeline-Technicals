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
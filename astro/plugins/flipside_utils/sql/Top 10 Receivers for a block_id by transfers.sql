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
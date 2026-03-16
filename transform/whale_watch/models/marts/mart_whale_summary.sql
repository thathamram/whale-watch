with enriched as (
       select * from {{ ref('int_whale_transactions_enriched') }}
   )

   select
       block_number,
       count(*) as num_whale_txs,
       sum(value_eth) as total_eth_moved,
       sum(case when flow_type = 'exchange_inflow' then value_eth else 0 end) as
 exchange_inflow_eth,
       sum(case when flow_type = 'exchange_outflow' then value_eth else 0 end) as
 exchange_outflow_eth,
       sum(case when flow_type = 'wallet_transfer' then value_eth else 0 end) as
 wallet_transfer_eth,
       count(distinct from_address) as unique_senders,
       count(distinct to_address) as unique_receivers
   from enriched
   group by block_number
   order by block_number desc

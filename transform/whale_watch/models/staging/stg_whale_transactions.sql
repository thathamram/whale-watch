with raw as (
       select * from raw_whale_transactions
   )

   select
       tx_hash,
       from_address,
       to_address,
       value_eth,
       block_number,
       ingested_at,
       case
           when value_eth >= 10000 then '🐋 mega_whale'
           when value_eth >= 1000 then '🐬 dolphin'
           when value_eth >= 100 then '🐟 fish'
       end as whale_tier
   from raw

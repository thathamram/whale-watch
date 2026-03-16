with staged as (
       select * from {{ ref('stg_whale_transactions') }}
   ),

   enriched as (
       select
           tx_hash,
           from_address,
           to_address,
           value_eth,
           block_number,
           whale_tier,
           ingested_at,

           -- Is this going TO or FROM a known exchange?
           case
               when to_address in (
                   '0x28c6c06298d514db089934071355e 5743bf21d60',  -- Binance
                   '0x21a31ee1afc51d94c2efccaa2092a d1028285549',  -- Binance
                   '0xdfd5293d8e347dfe59e90efd55b29 56a1343963d',  -- Binance
                   '0x56eddb7aa87536c09ccc279347359 9fd21a8b17f',  -- Coinbase
                   '0xa9d1e08c7793af67e9d92fe308d56 97fb81d3e43'   -- Coinbase
               ) then 'exchange_inflow'
               when from_address in (
                   '0x28c6c06298d514db089934071355e 5743bf21d60',
                   '0x21a31ee1afc51d94c2efccaa2092a d1028285549',
                   '0xdfd5293d8e347dfe59e90efd55b29 56a1343963d',
                   '0x56eddb7aa87536c09ccc279347359 9fd21a8b17f',
                   '0xa9d1e08c7793af67e9d92fe308d56 97fb81d3e43'
               ) then 'exchange_outflow'
               else 'wallet_transfer'
           end as flow_type

       from staged
   )

   select * from enriched

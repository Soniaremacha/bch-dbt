{{ config(
    materialized = 'table',
    schema = 'staging',
    alias = 'stg_bch_transactions_last_3m',
    partition_by = { "field": "block_timestamp", "data_type": "timestamp", "granularity": "day" }
) }}

with src as (
  select *
  from {{ source('bch','transactions') }}
),
maxd as (
  -- último timestamp disponible en la fuente
  select max(block_timestamp) as max_ts
  from src
),
windowed as (
  -- creamos tx_hash y calculamos rn para quedarnos con 1 fila por hash
  select
    s.*,
    s.hash as tx_hash,
    row_number() over (
      partition by s.hash
      order by s.block_timestamp desc, s.hash
    ) as rn
  from src s
  cross join maxd m
  where s.block_timestamp >= timestamp_sub(m.max_ts, interval 90 day)  -- ~3 meses
)

select * except(rn)
from windowed
where rn = 1

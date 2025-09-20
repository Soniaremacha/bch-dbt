-- Expandimos outputs -> una fila por dirección 
with outputs_expanded as (
  select
    o.transaction_hash,
    o.`index` as output_index,   
    o.value,
    addr as address
  from {{ source('bch', 'outputs') }} o,
  unnest(o.addresses) as addr
  where addr is not null
),

-- Direcciones con coinbase (minería)
coinbase_addrs as (
  select distinct oe.address
  from outputs_expanded oe
  join {{ source('bch', 'transactions') }} tx
    on tx.hash = oe.transaction_hash
  where tx.is_coinbase = true
),

-- Outputs gastados: inputs referencian (tx_hash, out_index)
spent_outputs as (
  select
    i.spent_transaction_hash as tx_hash,
    i.spent_output_index     as out_index
  from {{ source('bch', 'inputs') }} i
  where i.spent_transaction_hash is not null
),

-- UTXOs actuales = outputs (expandidos) sin correspondencia en inputs
unspent_outputs as (
  select
    oe.address,
    oe.value,
    oe.transaction_hash,
    oe.output_index
  from outputs_expanded oe
  left join spent_outputs s
    on s.tx_hash   = oe.transaction_hash
   and s.out_index = oe.output_index
  where s.tx_hash is null
),

-- Balance por dirección (satoshis)
balances as (
  select
    address,
    sum(value) as balance_sats
  from unspent_outputs
  group by 1
)

-- Resultado final: BCH y exclusión de coinbase
select
  b.address,
  b.balance_sats / 1e8 as balance_bch
from balances b
left join coinbase_addrs c
  on b.address = c.address
where c.address is null

with double_entry_book as (
    select to_address as address, value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)

    union all

    select from_address as address, -value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)

    union all

    select mine as address, sum(float(receipt_gas_used) * float(gas_price)) as value
    from `bigquery-public-data.crypto_ethereum.transactions` as transactions
    join `bigquery-public-data.crypto_ethereum.block` as blocks on blocks.number = transactions.block_number
    group by blocks.miner
    union all
    select from_address as address, -(float(receipt_gas_used) * float(gas_price)) as value
    from `bigquery-public-data.crypto_ethereum.transaction`
)
select IFNULL(address, Pow(10, (SELECT max(LENGTH(address)) from double_entry_book))), sum(value) as balance
from double_entry_book
group by address
order by balance desc
limit 1000
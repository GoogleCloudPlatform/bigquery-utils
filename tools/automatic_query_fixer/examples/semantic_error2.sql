WITH double_entry_book AS (
   -- debits
   SELECT array_to_sting(input.addresses, ",") as address, inputs.type, -inputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.input` as inputs
   UNION ALL
   -- credits
   SELECT array_to_string(outputs.addresses, ",") as address, outputs.type, outputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.output` as outputs
)
SELECT address, type, sum(value) as balance,  TO_BASE32(address) as byte_address, TO_BASE64(address) as byte_address
FROM double_entry_book
GROUP BY address
ORDER BY balance DESC
LIMIT 1000
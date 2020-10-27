SELECT DIV(created_data, 1000000000), street_number
FROM `bigquery-public-data.austin_311.311_requests`
where  MOD(street_number, 1000) > 100
LIMIT 10
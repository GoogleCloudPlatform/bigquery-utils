select
    'chat' as table_name,
    instance_id,
    sequence_id,
    service_id,
    max_sequence_id_on_instance,
    TIMESTAMP('2023-02-28 08:00:00') as event_date,
    event_name
from
    [sc-analytics:prod_analytics_chat.daily_events_20230228]
where
    instance_id is NOT NULL
    AND instance_id like '%'
    AND sequence_id is NOT NULL
    AND abs(hash(instance_id)) % 100=1
    
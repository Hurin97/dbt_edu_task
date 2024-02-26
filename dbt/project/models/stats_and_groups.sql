{{ config(materialized='incremental') }}

Select Period as period_,
Series_reference as series_reference,
Status,
Group_
from test.cards_stats
where period between '{{var('first_date')}}' and '{{var('last_date')}}'
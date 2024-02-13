{{ config(materialized='table') }}

Select Period as period_,
Series_reference as series_reference,
Data_value as data_val
from test.cards_stats
where period between '{{var('first_date')}}' and '{{var('last_date')}}'
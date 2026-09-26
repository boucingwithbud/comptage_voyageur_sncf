{{ config(
    materialized='table',
    comment='Dimension calendrier quotidienne, du 1er janvier 2000 au 31 décembre 2030.'
) }}

with dates as (
    select explode(
        sequence(to_date('2000-01-01'), to_date('2030-12-31'), interval 1 day)
    ) as date_day
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    concat(year(date_day), '-Q', quarter(date_day)) as year_quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    date_format(date_day, 'MMM') as month_name_short,
    date_trunc('month', date_day)::date as month_start_date,
    last_day(date_day) as month_end_date,
    dayofmonth(date_day) as day_of_month,
    dayofyear(date_day) as day_of_year,
    ((dayofweek(date_day) + 5) % 7) + 1 as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    date_format(date_day, 'E') as day_name_short,
    weekofyear(date_day) as week_of_year,
    date_trunc('week', date_day)::date as week_start_date,
    date_add(date_trunc('week', date_day)::date, 6) as week_end_date,
    date_day = current_date() as is_today,
    date_day = last_day(date_day) as is_month_end,
    ((dayofweek(date_day) + 5) % 7) + 1 in (6, 7) as is_weekend
from dates
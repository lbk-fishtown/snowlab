{% snapshot orders_scd2 %}
    {{
        config(
            unique_key='o_orderkey',
            strategy='check',
            check_cols='all',
            target_schema='dbt_lbkfishtown'
        )
    }}

    select * from {{ source('tpch', 'orders') }}
 {% endsnapshot %}
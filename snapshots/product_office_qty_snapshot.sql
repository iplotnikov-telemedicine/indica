{% snapshot product_office_qty_snapshot %}

    {{
        config(
          strategy='timestamp',
          target_schema='test',
          unique_key='comp_id || poq_id',
          updated_at='sync_updated_at',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('stg_io__product_office_qty') }}

{% endsnapshot %}
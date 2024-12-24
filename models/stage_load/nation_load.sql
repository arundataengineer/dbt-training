{{
    config (
            materialized = 'incremental',
            database = var('tgt_db'),
            schema = var('stg_sch'),
            alias = 'STG_NATION',
            unique_key = 'N_NATIONKEY',
            tags = ['STAGING']
    )
}}

SELECT 
    N_NATIONKEY,
    N_NAME,
    N_REGIONKEY,
    N_COMMENT,
    CURRENT_TIMESTAMP() AS LDTS,
    'Static Reference Data' AS RSCR
FROM {{ source('snowflake_sample_db','NATION')}}
WHERE 
{% if is_incremental() %}
LDTS >= (SELECT COALESCE(MAX(LDTS),'1900-01-01') FROM {{this}})
{% endif %}




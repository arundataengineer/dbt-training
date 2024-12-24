{{
    config (
            materialized = 'incremental',
            database = var('tgt_db'),
            schema = var('stg_sch'),
            alias = 'STG_REGION',
            unique_key = 'R_REGIONKEY',
            tags = ['STAGING']
    )
}}

SELECT
    R_REGIONKEY,
    R_NAME,
    R_COMMENT,
    CURRENT_TIMESTAMP() AS ldts,
    'Static Reference Data' AS rscr
FROM {{source('snowflake_sample_db','REGION')}}
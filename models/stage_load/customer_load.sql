{{
    config(
            materialized = 'incremental',
            unique_key = 'C_CUSTKEY',
            database = var('tgt_db'),
            schema = var('stg_sch'),
            alias = 'stg_customer',
            tags = ['STAGING']

    )
}}

SELECT
    C_CUSTKEY, 
    C_NAME, 
    C_ADDRESS, 
    C_NATIONKEY, 
    C_PHONE, 
    C_ACCTBAL, 
    C_MKTSEGMENT, 
    C_COMMENT,
    CURRENT_TIMESTAMP() AS LDTS,
    'Customers System' AS RSCR
FROM {{source('snowflake_sample_db','CUSTOMER')}}
WHERE C_CUSTKEY = 15001
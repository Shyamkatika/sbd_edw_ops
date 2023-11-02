{{ config(materialized='table' , transient=false, full_refresh=true, schema='FACTS' )}}

SELECT DISTINCT * FROM
(
SELECT * FROM {{ref('SRC_FACT_INVENTORY_OH_SNAPSHOT_SAPC11_HIST')}}
UNION ALL
SELECT * FROM {{ref('SRC_FACT_INVENTORY_OH_SNAPSHOT_SAPE03_HIST')}}
)
select
UPPER(CONCAT(coalesce(BDRESERVA.bd_domain::VARCHAR,''),'~',coalesce(BDRESERVA.bd_cli::VARCHAR,''))) AS INVTY_RSRV_HDR_KEY,
'QADBR' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(BDRESERVA.bd_domain::VARCHAR,''),'~',coalesce(BDRESERVA.bd_cli::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
BDRESERVA.LOADDTS AS {{var('column_z3loddtm')}},
BDRESERVA.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},

case when UPPER(BDRESERVA.bd_esp) in ('NO','FALSE','0') or UPPER(BDRESERVA.bd_esp) is null
or length(TRIM(BDRESERVA.bd_esp))<1 then 'N' when UPPER(BDRESERVA.bd_esp) in ('YES','TRUE','1')
then 'Y' else 'U' end as CUST_REQ_FLAG,

case when UPPER(BDRESERVA.bd_grupo) in ('NO','FALSE','0') or UPPER(BDRESERVA.bd_grupo) is null
or length(TRIM(BDRESERVA.bd_grupo))<1 then 'N' when UPPER(BDRESERVA.bd_grupo) in ('YES','TRUE','1')
then 'Y' else 'U' end as CUST_GRP_FLAG,

BDRESERVA.bd_ped AS ORD_START_TYP,
BDRESERVA.bd_lugar AS PROD_SLOC_ID,
BDRESERVA.bd_domain as DOMAIN_CD,
BDRESERVA.bd_cli as CO_HOLDING,
BDRESERVA.bd_nome AS CUST_NAME,
BDRESERVA.bd_pri AS PRYRT_NBR

FROM {{source('QADBR','BDRESERVA')}} BDRESERVA
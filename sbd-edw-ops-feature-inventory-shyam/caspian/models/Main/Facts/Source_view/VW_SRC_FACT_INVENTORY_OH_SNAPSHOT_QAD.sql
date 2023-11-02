SELECT
OH.INVTY_ON_HAND_SNAPSHOT_KEY AS INVTY_ON_HAND_SNAPSHOT_KEY
,OH.SRC_SYS_KEY
,OH.RCRD_HASH_KEY
,OH.DEL_FROM_SRC_FLAG
,'{{model.name}}' AS {{var('column_ETL_INS_PID')}}
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}}
,'{{model.name}}' AS {{var('column_ETL_UPD_PID')}}
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}}
,OH.EFF_DTE  AS EFF_DTE
,OH.EXPR_DTE AS EXPR_DTE
,OH.ETL_UPD_DTE AS ZONE4_ETL_UPD_DTE
,OH.ETL_INS_PID AS ZONE4_ETL_INS_PID
,OH.ZONE3_LOD_DTE
,OH.PROD_KEY
,OH.LOC_KEY
,OH.SLOC_ID
,OH.STORG_BIN_ID
,OH.STANDARD_PRICE
,OH.VALUATED_UNRSTR_STK_QTY
,OH.VALUATED_UNRSTR_STK_AMT
,OH.CRNCY_KEY
,OH.CO_KEY
FROM
(SELECT * FROM 
(select 
FACT.INVTY_ON_HAND_SNAPSHOT_KEY as	INVTY_ON_HAND_SNAPSHOT_KEY,
FACT.SRC_SYS_KEY	as	SRC_SYS_KEY,
FACT.RCRD_HASH_KEY	as	RCRD_HASH_KEY,
FACT.DEL_FROM_SRC_FLAG	as	DEL_FROM_SRC_FLAG,
FACT.EFF_DTE	as	EFF_DTE,
FACT.EXPR_DTE as EXPR_DTE,
FACT.ETL_UPD_DTE	as	ETL_UPD_DTE,
FACT.ETL_INS_PID	as	ETL_INS_PID,
FACT.ZONE3_LOD_DTE	as	ZONE3_LOD_DTE,
FACT.PROD_KEY	as	PROD_KEY,
FACT.LOC_KEY	as	LOC_KEY,
FACT.SLOC_ID	as	SLOC_ID,
FACT.STORG_BIN_ID 	as	STORG_BIN_ID,
PC.STANDARD_PRICE as STANDARD_PRICE,
FACT.PROD_NET_ON_HAND_QTY	as	VALUATED_UNRSTR_STK_QTY,  
coalesce(PC.STANDARD_PRICE,0)  * coalesce(FACT.PROD_NET_ON_HAND_QTY,0) as VALUATED_UNRSTR_STK_AMT,
CURR.CRNCY_KEY as CRNCY_KEY,
CURR.CO_KEY as CO_KEY
from {{source('CONSOLIDATED_PROD','EDW_INVENTORY_ON_HAND_SNAPSHOT')}}  FACT
left join (select distinct SRC_SYS_KEY,PROD_KEY,LOC_KEY,TOT_COST_AMT,EFF_DTE,EXPR_DTE
,upper(COST_TYP_LKEY) as COST_TYP_LKEY
,coalesce(TOT_COST_AMT,0) as STANDARD_PRICE  
from {{source('CONSOLIDATED_PROD','EDW_PRODUCT_COST')}}   
where  SRC_SYS_KEY like 'QAD%' and upper(COST_TYP_LKEY) IN ('AVERAGE','STANDARD')
QUALIFY RANK() OVER(PARTITION BY SRC_SYS_KEY,PROD_KEY,LOC_KEY ORDER BY upper(COST_TYP_LKEY) asc)=1) PC
on  FACT.SRC_SYS_KEY = PC.SRC_SYS_KEY
and FACT.PROD_KEY = PC.PROD_KEY
and FACT.LOC_KEY = PC.LOC_KEY
and (FACT.EXPR_DTE > PC.EFF_DTE and  FACT.EXPR_DTE <= PC.EXPR_DTE) 
Left join (Select DL.SRC_SYS_KEY,DL.LOC_KEY,CO.CRNCY_KEY,CO.CO_KEY from {{source('CONSOLIDATED_PROD','VW_EDW_LOCATION')}} DL
Left join {{source('CONSOLIDATED_PROD','VW_EDW_COMPANY')}} CO  
on DL.SRC_SYS_KEY = CO.SRC_SYS_KEY
and DL.CO_KEY = CO.CO_KEY
where DL.SRC_SYS_KEY like 'QAD%') CURR 
ON
upper(CONCAT(coalesce(FACT.SRC_SYS_KEY::VARCHAR,'#'),'~',coalesce(FACT.LOC_KEY::VARCHAR,'#'))) =
upper(CONCAT(coalesce(CURR.SRC_SYS_KEY::VARCHAR,'#'),'~',coalesce(CURR.LOC_KEY::VARCHAR,'#')))
where FACT.SRC_SYS_KEY like 'QAD%')
)OH

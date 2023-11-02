with PHYINVCT_MAR as( SELECT 
UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_MAR')) AS INVTY_CYCLE_CNT_KEY,
'INFORXA' AS {{var('column_srcsyskey')}},
md5(UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_MAR'))) AS {{var('column_rechashkey')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_CREATE_USERID')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_UPD_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
LOADDTS AS {{var('column_z3loddtm')}},
CONCAT('MAR','~',CNTLOC)  as LOC_KEY,
CNTPRT  as PROD_KEY,
CNTUOM  as BASE_UNIT_QTY,
CNTSUB  as CYCLE_CNT_CLS_KEY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_BATCH_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CNT_CMPLT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_ACT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_UPD_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_ENTR_DTE,
CNTQTY  as COUNTED_PROD_QTY,
CNTLQT  as PRE_CNT_CATCH_QTY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_DTE,
CNTSUC  as PROD_UNIT_COST,
CNTPRT  as PROD_ID
FROM
--  {{source("INFORXA", "PHYINVCT_MAR")}}
(select CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB,MAX(LOADDTS) AS LOADDTS,SUM(CNTLQT) AS CNTLQT ,SUM(CNTQTY) AS CNTQTY,SUM(CNTSUC) AS CNTSUC from {{source("INFORXA", "PHYINVCT_MAR")}}
 GROUP BY CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB)
),
PHYINVCT_NOG as( SELECT 
UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_NOG')) AS INVTY_CYCLE_CNT_KEY,
'INFORXA' AS {{var('column_srcsyskey')}},
md5(UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_NOG'))) AS {{var('column_rechashkey')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_CREATE_USERID')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_UPD_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
LOADDTS AS {{var('column_z3loddtm')}},
CONCAT('NOG','~',CNTLOC)  as LOC_KEY,
CNTPRT  as PROD_KEY,
CNTUOM  as BASE_UNIT_QTY,
CNTSUB  as CYCLE_CNT_CLS_KEY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_BATCH_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CNT_CMPLT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_ACT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_UPD_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_ENTR_DTE,
CNTQTY  as COUNTED_PROD_QTY,
CNTLQT  as PRE_CNT_CATCH_QTY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_DTE,
CNTSUC  as PROD_UNIT_COST,
CNTPRT  as PROD_ID
--  from  {{source("INFORXA", "PHYINVCT_NOG")}}    
FROM                                
(select CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB,MAX(LOADDTS) AS LOADDTS,SUM(CNTLQT) AS CNTLQT ,SUM(CNTQTY) AS CNTQTY,SUM(CNTSUC) AS CNTSUC from {{source("INFORXA", "PHYINVCT_NOG")}}
 GROUP BY CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB )
),     
PHYINVCT_TUP as( SELECT 
UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_NOG')) AS INVTY_CYCLE_CNT_KEY,
'INFORXA' AS {{var('column_srcsyskey')}},
md5(UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_NOG'))) AS {{var('column_rechashkey')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_CREATE_USERID')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_UPD_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
LOADDTS AS {{var('column_z3loddtm')}},
CONCAT('TUP','~',CNTLOC)  as LOC_KEY,
CNTPRT  as PROD_KEY,
CNTUOM  as BASE_UNIT_QTY,
CNTSUB  as CYCLE_CNT_CLS_KEY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_BATCH_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CNT_CMPLT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_ACT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_UPD_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_ENTR_DTE,
CNTQTY  as COUNTED_PROD_QTY,
CNTLQT  as PRE_CNT_CATCH_QTY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_DTE,
CNTSUC  as PROD_UNIT_COST,
CNTPRT  as PROD_ID
--  from  {{source("INFORXA", "PHYINVCT_TUP")}} 
FROM
  (select CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB,MAX(LOADDTS) AS LOADDTS,SUM(CNTLQT) AS CNTLQT ,SUM(CNTQTY) AS CNTQTY,SUM(CNTSUC) AS CNTSUC from {{source("INFORXA", "PHYINVCT_TUP")}}
  GROUP BY CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB )
), 
PHYINVCT_WIL as( SELECT 
UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_WIL')) AS INVTY_CYCLE_CNT_KEY,
'INFORXA' AS {{var('column_srcsyskey')}},
md5(UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''),'~','PHYINVCT_WIL'))) AS {{var('column_rechashkey')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_CREATE_USERID')}},
cast(CNTDTE as TIMESTAMP_NTZ(9))  AS {{var('column_SRC_RCRD_UPD_DTE')}},
CNTUSR  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
LOADDTS AS {{var('column_z3loddtm')}},
CONCAT('WIL','~',CNTLOC)  as LOC_KEY,
CNTPRT  as PROD_KEY,
CNTUOM  as BASE_UNIT_QTY,
CNTSUB  as CYCLE_CNT_CLS_KEY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_BATCH_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CNT_CMPLT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_ACT_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_UPD_DTE,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_SYS_ENTR_DTE,
CNTQTY  as COUNTED_PROD_QTY,
CNTLQT  as PRE_CNT_CATCH_QTY,
cast(CNTDTE as TIMESTAMP_NTZ(9))   as CYCLE_CNT_DTE,
CNTSUC  as PROD_UNIT_COST,
CNTPRT  as PROD_ID
--  from  {{source("INFORXA", "PHYINVCT_WIL")}}
 FROM
 (select CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB,MAX(LOADDTS) AS LOADDTS,SUM(CNTLQT) AS CNTLQT ,SUM(CNTQTY) AS CNTQTY,SUM(CNTSUC) AS CNTSUC from {{source("INFORXA", "PHYINVCT_WIL")}} 
 GROUP BY CNTDTE,CNTPRT,CNTLOC,CNTUSR,CNTUOM,CNTSUB)
) ,
PHYINVCT AS(
    SELECT * FROM PHYINVCT_MAR
    UNION
    SELECT * FROM PHYINVCT_NOG
    UNION
    SELECT * FROM PHYINVCT_TUP
    UNION
    SELECT * FROM PHYINVCT_WIL 
)
    SELECT * FROM PHYINVCT


-- WHERE CNTDTE ='1180417' 
-- and CNTPRT = '711-05489'
-- and CNTLOC = '05489' AND CNTUSR = 'JPOWEL'

-- 1180417~711-05489~ST01~JPOWEL~EA~

-- UPPER(concat(COALESCE(trim(CNTDTE)::VARCHAR,''),'~',COALESCE(trim(CNTPRT)::VARCHAR,''),'~',COALESCE(trim(CNTLOC)::VARCHAR,''),'~',
-- COALESCE(trim(CNTUSR)::VARCHAR,''),'~',COALESCE(trim(CNTUOM)::VARCHAR,''),'~',COALESCE(trim(CNTSUB)::VARCHAR,''))) AS INVTY_CYCLE_CNT_KEY,
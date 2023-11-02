With FCST as 
(select distinct
UPPER(CONCAT(COALESCE(DMDUNIT::VARCHAR,''),'~',COALESCE(DMDGROUP::VARCHAR,''), '~', COALESCE(LOC::VARCHAR,''),
'~', COALESCE(STARTDATE::VARCHAR,''),'~', COALESCE(TYPE::VARCHAR,''),'~', COALESCE(FCSTID::VARCHAR,''),
'~', COALESCE(MODEL::VARCHAR,''))) AS DMD_FCST_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(DMDUNIT::VARCHAR,''),'~',COALESCE(DMDGROUP::VARCHAR,''), '~', COALESCE(LOC::VARCHAR,''),
'~', COALESCE(STARTDATE::VARCHAR,''),'~', COALESCE(TYPE::VARCHAR,''),'~', COALESCE(FCSTID::VARCHAR,''),
'~', COALESCE(MODEL::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
LOADDTS as {{var('column_z3loddtm')}},
LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
'CURRENT' as DMD_FCST_CTGY, 
DMDGROUP as DMD_GRP_KEY,
DMDUNIT as DMD_UNIT_KEY,
DUR/1440 as FCST_DUR,
FCSTID as FCST_ID_TYP,
LEWMEANQTY as MEAN_FCST_QTY,
LOC as LOC_KEY,
MARKETMGRVERSIONID as MKT_MGR_VER_NBR,
QTY as FCST_QTY,
MODEL as PLNG_MODEL_NAME,
STARTDATE as FCST_START_DTE,
TYPE as FCST_TYP,
'EA' as UOM_KEY
--'NULL' as PROD_KEY,
--'NULL' as FCST_STREAM_KEY
from {{source('BLUEYONDER','FCST')}}
 order by DMDUNIT,DMDGROUP,LOC,STARTDATE,TYPE , FCSTID, MODEL),
 
 FCDRAFT as 
(select DISTINCT
UPPER(CONCAT(COALESCE(DMDUNIT::VARCHAR,''),'~',COALESCE(DMDGROUP::VARCHAR,''), '~', COALESCE(LOC::VARCHAR,''),
'~', COALESCE(STARTDATE::VARCHAR,''),'~', COALESCE(TYPE::VARCHAR,''),'~', COALESCE(FCSTID::VARCHAR,''),
'~', COALESCE(MODEL::VARCHAR,''))) AS DMD_FCST_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(DMDUNIT::VARCHAR,''),'~',COALESCE(DMDGROUP::VARCHAR,''), '~', COALESCE(LOC::VARCHAR,''),
'~', COALESCE(STARTDATE::VARCHAR,''),'~', COALESCE(TYPE::VARCHAR,''),'~', COALESCE(FCSTID::VARCHAR,''),
'~', COALESCE(MODEL::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
LOADDTS as {{var('column_z3loddtm')}},
LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
'DRAFT' as DMD_FCST_CTGY, 
DMDGROUP as DMD_GRP_KEY,
DMDUNIT as DMD_UNIT_KEY,
DUR/1440 as FCST_DUR,
FCSTID as FCST_ID_TYP,
LEWMEANQTY as MEAN_FCST_QTY,
LOC as LOC_KEY,
MARKETMGRVERSIONID as MKT_MGR_VER_NBR,
QTY as FCST_QTY,
MODEL as PLNG_MODEL_NAME,
STARTDATE as FCST_START_DTE,
TYPE as FCST_TYP,
'EA' as UOM_KEY
---'NULL' as PROD_KEY,
--'NULL' as FCST_STREAM_KEY
from  {{source('BLUEYONDER','FCSTDRAFT')}}
 order by DMDUNIT,DMDGROUP,LOC,STARTDATE,TYPE , FCSTID, MODEL),
 
 FCDRAFT_ONLY as (
   select distinct * from FCDRAFT FD 
   where NOT EXISTS (
     SELECT DMD_FCST_KEY from FCST F
     where F.DMD_FCST_KEY=FD.DMD_FCST_KEY  
   )
),

 FCREPORT as (
 select distinct * from  FCST 
  union all
 select distinct * from FCDRAFT_ONLY  
 )
 
 select distinct * from FCREPORT 

{% macro Scd2_Steps(Dest_Table,Source_view,vzone4dte,vload_type,tgtpk,vsid,srctblname,srcschema) %}

 {% set tgttable_tmp1 = Dest_Table+"_TMP1"  %}
 {% set tgttable_tmp2 = Dest_Table+"_TMP2"  %}
 {% set tgttable_tmp = Dest_Table+"_TMP"  %}
 {% set tgttable = Dest_Table  %}
 {% set db_destschema = var('db_factsschema') %} 
 {% set db_srcviewdb = var('db_srcviewdb')  %} 
 {% set db_srcschema = var('db_srcschema')  %}  
 {% set metadateloaddt = var('table_metadateloaddt')  %}
 {% set column_nonpkhash = var('column_nonpkhash') %}
 {% set column_ETL_UPD_DTE = var('column_ETL_UPD_DTE') %}
 {% set vzone4dte = vzone4dte  %} 
 
   
 {{ log("Running  Scd2_Steps: " ~ tgttable_tmp1 ~ ", " ~ tgttable_tmp2 ~ ", " ~ tgttable ) }}
    
    {% set log_cnt%}
        SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{tgttable}}' and TABLE_SCHEMA ='{{db_destschema}}'
    {% endset %}
    {%- set resultqry = run_query(log_cnt) -%}
    {%- set rescheck_main_table    = resultqry.columns[0].values()[0] -%}
      
     
            
      BEGIN;

              {% set Create_tmp_incrdte %}
                    CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{tgttable_tmp}} copy grants
                    as SELECT * FROM {{var('db_srcviewdb')}}.{{Source_view}} where (ZONE4_ETL_UPD_DTE > '{{vzone4dte}}' OR EXPR_DTE > '{{vzone4dte}}') AND DEL_FROM_SRC_FLAG<>'Y';
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Create_tmp_incrdte) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}

              {{ log(tgttable_tmp~":Table created" ) }}            
                  
              {% set insert_stmnt %}
                    INSERT INTO {{var('db_factsschema')}}.{{tgttable}} {{get_columns(tgttable_tmp,var('db_srcviewdb'))|replace('"', '')}}
                    SELECT {{get_columns(tgttable_tmp,var('db_srcviewdb'))|replace('"', '')|replace('(', '')|replace(')', '')}} FROM  {{var('db_srcviewdb')}}.{{tgttable_tmp}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(insert_stmnt) %}
                    {{ log("SCD2 Insert COMPLETED " ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %}
              /*Delete logic*/
              {% if target.database == 'DEV_EDW' %} 
                    {% set srcdb='TEST_EDW' %}
              {% elif target.database == 'TEST_EDW' -%}
                    {% set srcdb='PROD_EDW' %}
              {% elif target.database == 'PROD_EDW' %}
                    {% set srcdb='PROD_EDW' %}
              {% elif target.database == 'UAT_EDW' %}
                    {% set srcdb='PROD_EDW' %}
              {% else -%} 
                    {% set srcdb='INVALID_DATABASE' %}
              {% endif %}

              
              {% set Get_read_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp}}
              {% endset %}

              {% if execute %}

                    {%- set Readcnt = run_query(Get_read_cnt).columns[0].values()[0] -%}
                    {%- set  Readcnt = Readcnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD2-Readcnt: " ~ Readcnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}                 

              {% set Get_ins_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp}} Where {{var('column_verexpirydt')}} = '9999-12-31 00:00:00.000';
              {% endset %}

              {% if execute %}

                    {%- set Inscnt = run_query(Get_ins_cnt).columns[0].values()[0] -%}
                    {%- set  Inscnt = Inscnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD1-Inscnt: " ~ Inscnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}           

              {% set Get_upd_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp}} Where {{var('column_verexpirydt')}} <> '9999-12-31 00:00:00.000';
              {% endset %}

              {% if execute %}

                    {%- set Updcnt = run_query(Get_upd_cnt).columns[0].values()[0] -%}
                    {%- set Updcnt = Updcnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD1-Updcnt: " ~ Updcnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}                     


              {% set upd_metadate %}
                    UPDATE {{var('db_factsschema')}}.{{metadateloaddt}} SET  {{var('column_z4etlupddte')}} =
                    (SELECT MAX(ZONE3_LOD_DTE) FROM {{var('db_srcviewdb')}}.{{tgttable_tmp}})
                    WHERE {{var('column_met_tgttbl')}} = '{{Dest_Table}}'                    
                    and {{var('column_met_tgtschema')}} = '{{db_destschema}}'
                    and {{var('column_met_modelname')}} = '{{Source_view}}';
              {% endset %}                   

              
              {% if execute %}
                    {%- set result = run_query(upd_metadate) %}
                    {{ log(metadateloaddt~":Table Successfully Updated" ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %} 

              {% set check_main_table%}
                    SELECT COUNT(1) FROM {{var('db_factsschema')}}.{{var('table_logging')}} WHERE LOADSTATUS='SCD2-COMPLETED' AND MODEL_NAME='{{Source_view}}'
                    AND DSTTBLNAME='{{tgttable}}' AND MODEL_NAME LIKE '%SAP%';
              {% endset %} 

              {%- set resultqry = run_query(check_main_table) -%}
              {%- set rescheck_main_table    = resultqry.columns[0].values()[0] -%}

              {% if (rescheck_main_table==1)%}
                    {{ log("Validate the 1st successful scd2 run")}}
                    {% set merge_hist %}
                        MERGE INTO {{var('db_factsschema')}}.{{tgttable}} TGT USING 
                        (SELECT * FROM
                        (SELECT * FROM (select SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID,INVTY_ON_HAND_SNAPSHOT_KEY,EFF_DTE,
                        LEAD(EFF_DTE)over(partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by EXPR_DTE ASC) LD_EFF,
                        LEAD(ZONE4_ETL_INS_PID)over(partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by EXPR_DTE ASC)LD_PID,ZONE4_ETL_INS_PID,EXPR_DTE 
                        FROM {{var('db_factsschema')}}.{{tgttable}}) WHERE ZONE4_ETL_INS_PID LIKE '%HIST%') 
                        QUALIFY ROW_NUMBER() OVER(PARTITION BY SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID ORDER BY EFF_DTE DESC)=1)SRC 
                        ON TGT.INVTY_ON_HAND_SNAPSHOT_KEY=SRC.INVTY_ON_HAND_SNAPSHOT_KEY AND TGT.{{var('column_srcsyskey')}}=SRC.{{var('column_srcsyskey')}} 
                        AND TGT.ZONE4_ETL_INS_PID<>SRC.LD_PID AND TO_DATE(TGT.EFF_DTE)=TO_DATE(SRC.EFF_DTE) AND TGT.ZONE4_ETL_INS_PID=SRC.ZONE4_ETL_INS_PID 
                        WHEN MATCHED THEN UPDATE SET TGT.EXPR_DTE=SRC.LD_EFF;
              
                    {% endset %}

                    {% if execute %}
                        {%- set result = run_query(merge_hist) %} 
                        {{ log("Merge statement completed" )}}
                    {% else %} 
                        {{return(false)}}
                    {% endif %}
              {% else %}
                    {{ log("Update expr date not required")}}
              {% endif %}


      COMMIT;
      {{ Logging_Upd( Dest_Table,Source_view,vsid,'SCD2-COMPLETED',1,Readcnt,Inscnt,Updcnt,'N')}}

  {{ log("COMPLETED  Scd1_Steps:")}}
{% endmacro %}
  
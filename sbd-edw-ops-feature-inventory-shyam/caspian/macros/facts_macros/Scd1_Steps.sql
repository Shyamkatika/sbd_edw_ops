
{% macro Scd1_Steps(Dest_Table,Source_view,vzone4dte,vload_type,tgtpk,vsid,srctblname,srcschema) %}

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
 
   
 {{ log("Running  Scd1_Steps: " ~ tgttable_tmp1 ~ ", " ~ tgttable_tmp2 ~ ", " ~ tgttable ) }}
     
            
      BEGIN;

              {% set Create_tmp_incrdte %}
                    CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{tgttable_tmp}} copy grants
                    as SELECT md5(CONCAT({{Get_NonpkHashkey(Source_view,var('db_srcviewdb'),tgtpk)|replace('("', '')|replace('")', '')|replace('", "',',''~'',')|replace('~', '"~"')|replace('"', "'")}})) as {{column_nonpkhash}},* FROM {{var('db_srcviewdb')}}.{{Source_view}} where {{var('column_z4etlupddte')}} > '{{vzone4dte}}';
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Create_tmp_incrdte) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}

              {{ log(tgttable_tmp~":Table created" ) }}            
                  
              {% set merge_stmnt %}
                    MERGE INTO {{var('db_srcviewdb')}}.{{tgttable_tmp2}} DEST
                    USING {{var('db_srcviewdb')}}.{{tgttable_tmp}} SRC
                    ON  
                    SRC.{{var('column_rechashkey')}} = DEST.{{var('column_rechashkey')}} AND SRC.{{var('column_srcsyskey')}}=DEST.{{var('column_srcsyskey')}}
        
                    WHEN MATCHED AND SRC.{{var('column_nonpkhash')}} <> DEST.{{var('column_nonpkhash')}}  THEN UPDATE set
                    {{get_columnsSCD1(tgttable_tmp,var('db_srcviewdb'))|replace('(', '')|replace(')','')|replace('"', '')}}

                    WHEN NOT MATCHED THEN INSERT  
                    {{get_columns(tgttable_tmp,var('db_srcviewdb'))|replace('"', '')}}
                    VALUES 
                    {{get_columns(tgttable_tmp,var('db_srcviewdb'))|replace('"', '')}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(merge_stmnt) %}
                    {{ log("SCD1 Merge COMPLETED " ) }}
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

              {% if vzone4dte>'1900-01-01 00:00:00.000' %}
                    {% set del_stmnt %}
                        UPDATE {{var('db_srcviewdb')}}.{{tgttable_tmp2}} Z5_tmp_tbl set {{var('column_DEL_FROM_SRC_FLAG')}}='Y', 
                        {{column_ETL_UPD_DTE}} = CURRENT_TIMESTAMP::TIMESTAMP_NTZ,
                        {{var('column_z4etlupddte')}} = Z4_tbl.ETL_UPD_DTE
                        from (select * from {{srcdb}}.{{srcschema}}.{{srctblname}} 
                        where {{column_ETL_UPD_DTE}} > '{{vzone4dte}}' qualify row_number() over (partition by RCRD_HASH_KEY,SRC_SYS_KEY 
                        order by ETL_UPD_DTE desc )=1 )Z4_tbl
                        where Z4_tbl.{{var('column_DEL_FROM_SRC_FLAG')}}='Y' and Z4_tbl.RCRD_HASH_KEY = Z5_tmp_tbl.RCRD_HASH_KEY and 
                        Z4_tbl.SRC_SYS_KEY = Z5_tmp_tbl.SRC_SYS_KEY ;
                    {% endset %}

                    {% if execute %}
                        {%- set result = run_query(del_stmnt) %}
                        {{ log("Soft Deletes Completed" ) }}
                    {% else %} 
                        {{return(false)}}
                    {% endif %}     
              {% endif %}

              {% set Get_read_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp}}
              {% endset %}

              {% if execute %}

                    {%- set Readcnt = run_query(Get_read_cnt).columns[0].values()[0] -%}
                    {%- set  Readcnt = Readcnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD1-Readcnt: " ~ Readcnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}                 

              {% set Get_ins_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp2}} tmp2 inner join {{var('db_srcviewdb')}}.{{tgttable_tmp}} tmp
                    where tmp2.{{var('column_rechashkey')}} = tmp.{{var('column_rechashkey')}} and 
                    tmp2.{{var('column_nonpkhash')}} = tmp.{{var('column_nonpkhash')}}  and
                    tmp2.{{var('column_z4etlupddte')}} > '{{vzone4dte}}' and (tmp2.{{var('column_ETL_INS_DTE')}} =tmp2.{{var('column_ETL_UPD_DTE')}} and 
                    tmp.{{var('column_ETL_INS_DTE')}} = tmp2.{{var('column_ETL_INS_DTE')}})
              {% endset %}

              {% if execute %}

                    {%- set Inscnt = run_query(Get_ins_cnt).columns[0].values()[0] -%}
                    {%- set  Inscnt = Inscnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD1-Inscnt: " ~ Inscnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}           

              {% set Get_upd_cnt %}
                    select count(*) from {{var('db_srcviewdb')}}.{{tgttable_tmp2}} tmp2 inner join {{var('db_srcviewdb')}}.{{tgttable_tmp}} tmp
                    where tmp2.{{var('column_rechashkey')}} = tmp.{{var('column_rechashkey')}} and 
                    tmp2.{{var('column_nonpkhash')}} <> tmp.{{var('column_nonpkhash')}}  and
                    tmp2.{{var('column_z4etlupddte')}} > '{{vzone4dte}}' and (tmp2.{{var('column_ETL_INS_DTE')}} < tmp2.{{var('column_ETL_UPD_DTE')}} and 
                    tmp.{{var('column_ETL_UPD_DTE')}} = tmp2.{{var('column_ETL_UPD_DTE')}})
              {% endset %}

              {% if execute %}

                    {%- set Updcnt = run_query(Get_upd_cnt).columns[0].values()[0] -%}
                    {%- set Updcnt = Updcnt|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                    {{ log("SCD1-Updcnt: " ~ Updcnt) }}                    
              {% else %} 
                    {{return(false)}}
              {% endif %}                     

              {% set drop_actual %}
                    drop table if exists {{var('db_factsschema')}}.{{tgttable}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(drop_actual) %}
                    {{ log(tgttable~":Table Dropped" ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %}        

              {% set alter_tmp2 %}
                    ALTER TABLE IF EXISTS {{var('db_srcviewdb')}}.{{tgttable_tmp2}} rename to {{var('db_factsschema')}}.{{tgttable}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(alter_tmp2) %}
                    {{ log(tgttable_tmp2~":Table Successfully renamed to "~tgttable ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %} 

              {% set upd_metadate %}
                    UPDATE {{var('db_factsschema')}}.{{metadateloaddt}} SET  {{var('column_z4etlupddte')}} =
                    (SELECT MAX({{var('column_z4etlupddte')}}) FROM {{var('db_srcviewdb')}}.{{tgttable_tmp}})
                    WHERE {{var('column_met_tgttbl')}} = '{{Dest_Table}}'
                    and {{var('column_met_srcschema')}} = '{{db_srcschema}}'
                    and {{var('column_met_tgtschema')}} = '{{db_destschema}}'
                    and {{var('column_met_modelname')}} = '{{Source_view}}';
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(upd_metadate) %}
                    {{ log(metadateloaddt~":Table Successfully Updated" ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %} 
      COMMIT;
      {{ Logging_Upd( Dest_Table,Source_view,vsid,'SCD1-COMPLETED',1,Readcnt,Inscnt,Updcnt,'N')}}

  {{ log("COMPLETED  Scd1_Steps:")}}
{% endmacro %}
  
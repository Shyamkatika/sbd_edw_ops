{% macro Post_Load_Steps(Dest_Table,Source_view) %} 
 {% set tgttable_tmp1 = Dest_Table+"_TMP1"  %}
 {% set tgttable_tmp2 = Dest_Table+"_TMP2"  %}
 {% set tgttable = Dest_Table  %}
 {% set db_destschema = "'" ~ var('db_factsschema')  ~ "'"  %} 
 {% set db_srcviewdb = "'" ~ var('db_srcviewdb')  ~ "'"  %} 
   
 {{ log("Running  Post_Load_Steps: " ~ tgttable_tmp1 ~ ", " ~ tgttable_tmp2 ~ ", " ~ tgttable ) }}
 {{ log("db_destschema  variables display: " ~ db_destschema ) }}
 {{ log("db_srcviewdb   variables display: " ~ db_srcviewdb ) }}

    {% set table_query %}
            SELECT LOAD_TYPE, ZONE4_ETL_UPD_DTE,TGT_PRIMARY_KEY,SID, 
            CASE WHEN SRC_TABLENAME LIKE 'VW_%' THEN SUBSTR(SRC_TABLENAME,4)
                 ELSE  SRC_TABLENAME END as SRC_TABLENAME, SRC_SCHEMA
            from {{var('db_factsschema')}}.{{var('table_metadateloaddt')}} where TGT_TABLENAME ='{{tgttable}}' and MODEL_NAME ='{{Source_view}}';
    {% endset %}

    {% if execute %}
          {%- set vload_type = run_query(table_query).columns[0].values()[0] -%}
          {%- set  vload_type = vload_type|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{ log("Post_Load_Steps-vload_type: " ~ vload_type) }}

          {%- set vzone4dte = run_query(table_query).columns[1].values()[0] -%}
          {%- set  vzone4dte = vzone4dte|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{ log("Post_Load_Steps-vzone4dte: " ~ vzone4dte) }}

          {%- set tgtpk = run_query(table_query).columns[2].values()[0] -%}
          {%- set  tgtpk = tgtpk|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{ log("Post_Load_Steps-tgtpk: " ~ tgtpk) }}

          {%- set vsid = run_query(table_query).columns[3].values()[0] -%}
          {%- set  vsid = vsid|replace("'", '"')|replace("('", '')|replace("',)", '') -%} 
                {{ log("Pre_Load_Steps-vsid: " ~ vsid) }}  

          {%- set srctblname = run_query(table_query).columns[4].values()[0] -%}
          {%- set  srctblname = srctblname|replace("'", '"')|replace("('", '')|replace("',)", '') -%} 
                {{ log("Pre_Load_Steps-srctblname: " ~ srctblname) }} 

          {%- set srcschema = run_query(table_query).columns[5].values()[0] -%}
          {%- set  srcschema = srcschema|replace("'", '"')|replace("('", '')|replace("',)", '') -%} 
                {{ log("Pre_Load_Steps-srcschema: " ~ srcschema) }}                 

    {% else %}
                {{ Logging_Upd( Dest_Table,Source_view,vsid,'ERROR',3,0,0,'Post Load METADATA Read Failed')}}
                {{ log("Post_Load_Steps-failed: ") }}
                {{return( false) }}
    {% endif %}

    {% if vload_type=='FULLR' %}

            BEGIN;

                {% set trunc_tmp2 %}
                    truncate table {{var('db_srcviewdb')}}.{{tgttable_tmp2}};
                {% endset %}

                {% if execute %}
                    {%- set result = run_query(trunc_tmp2) %}
                {% else %} 
                    {{return(false)}}
                {% endif %}
        
                {% set Insert_tmp2 %}
                    insert into {{var('db_srcviewdb')}}.{{tgttable_tmp2}} {{get_columns(Source_view,var('db_srcviewdb'))|replace('"', '')}}
                     select {{get_columns(Source_view,var('db_srcviewdb'))|replace('"', '')|replace('(', '')|replace(')', '')}} from {{var('db_srcviewdb')}}.{{Source_view}};
                    
                {% endset %}

                {% if execute %}
                    {%- set result = run_query(Insert_tmp2) %}
                {% else %} 
                    {{return(false)}}
                {% endif %}

                {% set drop_actual %}
                    drop table {{var('db_factsschema')}}.{{tgttable}};
                {% endset %}

                {% if execute %}
                    {%- set result = run_query(drop_actual) %}
                {% else %} 
                    {{return(false)}}
                {% endif %}        

                {% set alter_tmp2 %}
                    ALTER TABLE IF EXISTS {{var('db_srcviewdb')}}.{{tgttable_tmp2}} rename to {{var('db_factsschema')}}.{{tgttable}};
                {% endset %}

                {% if execute %}
                    {%- set result = run_query(alter_tmp2) %}
                {% else %} 
                    {{return(false)}}
                {% endif %} 

                {% set drop_view %}
                    DROP VIEW IF EXISTS {{var('db_srcviewdb')}}.{{tgttable}};
                {% endset %}

                {% if execute %}
                    {%- set result = run_query(drop_view) %}
                {% else %} 
                    {{return(false)}}
                {% endif %} 

            COMMIT;
            {{ Logging_Upd( Dest_Table,Source_view,vsid,'FULLR-COMPLETED',1,Readcnt,Inscnt,0,'N')}}

    {% elif vload_type=='SCD1' %}
            {{ log("calling Scd1_Steps") }}
                {{ Scd1_Steps( Dest_Table,Source_view,vzone4dte,vload_type,tgtpk,vsid,srctblname,srcschema)}}
            {{ log("Scd1_Steps macro COMPLETED ") }}
    {% elif vload_type=='SCD2' %}
            {{ log("calling Scd2_Steps") }}
                {{ Scd2_Steps( Dest_Table,Source_view,vzone4dte,vload_type,tgtpk,vsid,srctblname,srcschema)}}  
            {{ log("Scd2_Steps macro COMPLETED ") }}            
    {% else %}
            {{ exceptions.raise_compiler_error("Only FULLR and SCD1 types of data loads are handled") }}       
            {{return( false) }}         
    {% endif %}
                        
{% endmacro %}
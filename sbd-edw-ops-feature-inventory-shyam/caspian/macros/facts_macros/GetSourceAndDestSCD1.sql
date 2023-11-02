{% macro get_columnsSCD1(table_name,dbtview) %}
      {% set new_query_tag = model.name+"_TMP1" %}
      /*{% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %}*/
      {% set etlInrt = "'" ~ var('column_ETL_INS_DTE')  ~ "'"  %}
      {% set nonpkhash = "'" ~ var('column_nonpkhash')  ~ "'"  %}
      {% set etlUpdt = "'" ~ var('column_ETL_UPD_DTE')  ~ "'"  %}
    
    {% set table_query %}
          SELECT concat('DEST.',COLUMN_NAME, CASE WHEN COLUMN_NAME={{etlUpdt}} THEN '=CURRENT_TIMESTAMP::TIMESTAMP_NTZ' ELSE   CONCAT('=SRC.',COLUMN_NAME) END)  
          FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{{table_name}}' AND  TABLE_SCHEMA={{dbtview}} 
          AND COLUMN_NAME NOT IN({{etlInrt}})  ORDER BY ORDINAL_POSITION 
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values() -%}
          {%- set  result = result|replace("'", '"') -%}
          {{ log("SCD1 query result :"~ result ) }}
           {{return( result )}}
    {% else %}
        {{return( false ) }}
    {% endif %}
    
{% endmacro %}
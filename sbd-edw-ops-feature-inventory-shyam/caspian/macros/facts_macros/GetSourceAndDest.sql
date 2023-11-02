{% macro get_columns(table_name,dbtview) %}
/*{% set new_query_tag = model.name+"_TMP1" %}*/
/*{% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %}*/
    
    {% set table_query %}
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME='{{table_name}}' AND  TABLE_SCHEMA={{dbtview}}   ORDER BY ORDINAL_POSITION
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values() -%}
          {%- set  result = result|replace("'", '"') -%}
          {{ log("normal query result :"~ result ) }}
           {{return( result )}}
    {% else %}
        {{return( false ) }}
    {% endif %}
    
{% endmacro %}

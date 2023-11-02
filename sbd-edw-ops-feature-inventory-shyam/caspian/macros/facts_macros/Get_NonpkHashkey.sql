{% macro Get_NonpkHashkey(table_name,dbtview,tgtpk) %}
 {% set column_rechashkey = var('column_rechashkey') %}
 {% set column_ETL_INS_PID = var('column_ETL_INS_PID') %} 
 {% set column_ETL_INS_DTE = var('column_ETL_INS_DTE') %} 
 {% set column_ETL_UPD_PID = var('column_ETL_UPD_PID') %} 
 {% set column_ETL_UPD_DTE = var('column_ETL_UPD_DTE') %}
 {% set column_z4etlupddte = var('column_z4etlupddte') %}
 {% set column_nonpkhash = var('column_nonpkhash') %}  
 
    
    {% set table_query %}
        SELECT concat('COALESCE(',COLUMN_NAME,'::VARCHAR,'''')') FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME='{{table_name}}' AND  TABLE_SCHEMA='{{dbtview}}'
        AND COLUMN_NAME not in ('{{tgtpk}}','{{column_rechashkey}}','{{column_ETL_INS_PID}}','{{column_ETL_INS_DTE}}','{{column_ETL_UPD_PID}}','{{column_ETL_UPD_DTE}}',
                        '{{column_z4etlupddte}}','{{column_nonpkhash}}')   ORDER BY ORDINAL_POSITION
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values() -%}
          {{ log("nonpkqueryresult unrefined result :"~ result ) }}
          
          {{return( result )}}
    {% else %}
          {{return( false ) }}
    {% endif %}
    
{% endmacro %}

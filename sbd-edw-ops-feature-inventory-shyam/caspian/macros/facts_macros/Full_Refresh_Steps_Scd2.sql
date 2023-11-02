
{% macro Full_Refresh_Steps_Scd2(Dest_Table,Source_view) %}

 {% set tgttable_tmp1 = Dest_Table+"_TMP1"  %}
 {% set tgttable_tmp2 = Dest_Table+"_TMP2"  %}
 {% set tgttable = Dest_Table  %}
 {% set srctable = Source_view  %}
 {% set db_destschema = var('db_factsschema') %} 
 {% set db_srcviewdb = var('db_srcviewdb')  %} 
 
   
 {{ log("Running  Full_Refresh_Steps: " ~ tgttable_tmp1 ~ ", " ~ tgttable_tmp2 ~ ", " ~ tgttable ) }}

    {% set check_main_table%}
        SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{tgttable}}' and TABLE_SCHEMA ='{{db_destschema}}'
    {% endset %}
    {%- set resultqry = run_query(check_main_table) -%}
    {%- set rescheck_main_table    = resultqry.columns[0].values()[0] -%}

    {% set check_tmp1_table%}
        SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{tgttable_tmp1}}' and TABLE_SCHEMA ='{{db_srcviewdb}}'
    {% endset %}
    {%- set resultqry = run_query(check_tmp1_table) -%}
    {%- set rescheck_tmp1_table    = resultqry.columns[0].values()[0] -%}

    {{ log("rescheck_main_table and rescheck_tmp1_table: " ~ rescheck_main_table ~ rescheck_tmp1_table)}}

    {% if (rescheck_main_table==0 and rescheck_tmp1_table==0 )%}
          {{ log("Checking if main and tmp1 tables exsits")}}
          {{ exceptions.raise_compiler_error(" Table does not exist; please create: " ~ tgttable) }}
    {% elif (rescheck_main_table==0 and rescheck_tmp1_table!=0 ) %}
          {{ log("Creating " ~ tgttable ~ " from " ~ tgttable_tmp1 )}}
          {% set Create_table %}
              create table {{var('db_factsschema')}}.{{tgttable}} clone {{var('db_srcviewdb')}}.{{tgttable_tmp1}} copy grants;
          {% endset %}

          {% if execute %}
              {%- set result = run_query(Create_table) %}
          {% else %} 
              {{return(false)}}
          {% endif %}

          {{ log("Table Created" )}}                

    {% endif %}

    {% set recheck_main_table %}
          SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{tgttable}}' and TABLE_SCHEMA ='{{db_destschema}}'
    {% endset %}
    {%- set resultqry1 = run_query(recheck_main_table) -%}
    {%- set resrecheck_main_table    = resultqry1.columns[0].values()[0] -%}
    {{ log("resrecheck_main_table-1:" ~resrecheck_main_table)}}
    
    {% if resrecheck_main_table!=0 %}
        {{ log("resrecheck_main_table-2:" ~resrecheck_main_table)}}
          BEGIN;
          
              {% set Drop_tmp1 %}
                    DROP TABLE IF EXISTS {{var('db_srcviewdb')}}.{{tgttable_tmp1}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Drop_tmp1) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}

              {% set Create_tmp1 %}
                    create table {{var('db_srcviewdb')}}.{{tgttable_tmp1}} clone {{var('db_factsschema')}}.{{tgttable}} copy grants;
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Create_tmp1) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}

              {% set Drop_tmp2 %}
                    DROP TABLE IF EXISTS {{var('db_srcviewdb')}}.{{tgttable_tmp2}};
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Drop_tmp2) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}
              
              {% set del_9999 %}                    
                    DELETE  FROM  {{var('db_factsschema')}}.{{tgttable}} WHERE {{var('column_verexpirydt')}} = '9999-12-31 00:00:00.000' AND ETL_INS_PID='{{srctable}}';
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(del_9999) %}
              {% else %} 
                    {{return(false)}}
              {% endif %}       

   
          COMMIT;
    {% else %}
        {{ log("resrecheck_main_table-Else part")}}
              {{ exceptions.raise_compiler_error(" Table does not exist; please create: " ~ tgttable) }}       
    {% endif %}

  {{ log("COMPLETED  Full_Refresh_Steps:")}}
{% endmacro %}
  
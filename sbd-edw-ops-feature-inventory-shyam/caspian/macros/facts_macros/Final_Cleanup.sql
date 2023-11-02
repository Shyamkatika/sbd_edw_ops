
{% macro Final_Cleanup(Dest_Table,Source_view) %}

 {% set tgttable_tmp = Dest_Table+"_TMP"  %}
 {% set tgttable = Dest_Table  %}
 {% set vw_tgttable = "VW_"+Dest_Table  %}
 {% set db_destschema = var('db_factsschema') %} 
 {% set db_srcviewdb = var('db_srcviewdb')  %} 
 {% set db_srcschema = var('db_srcschema')  %}  
 {% set metadateloaddt = var('table_metadateloaddt')  %}
 {% set vzone4dte = vzone4dte  %} 
 
   
 {{ log("Final_Cleanup: " ~ tgttable_tmp ) }}
     
    
      BEGIN;

              {% set Drop_tmp %}
                    DROP TABLE IF EXISTS {{var('db_srcviewdb')}}.{{tgttable_tmp}};              
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Drop_tmp) %}
                    {{ log(tgttable_tmp~":Table Dropped" ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %}


              {% set Drop_tmp2 %}
                    DROP VIEW IF EXISTS {{var('db_factsschema')}}.{{vw_tgttable}};              
              {% endset %}

              {% if execute %}
                    {%- set result = run_query(Drop_tmp2) %}
                    {{ log(vw_tgttable~":View Dropped" ) }}
              {% else %} 
                    {{return(false)}}
              {% endif %}

      COMMIT;

  {{ log("COMPLETED  Final Cleanup")}}
{% endmacro %}
  
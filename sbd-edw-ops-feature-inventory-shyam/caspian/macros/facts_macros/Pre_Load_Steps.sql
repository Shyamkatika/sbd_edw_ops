
{% macro Pre_Load_Steps(Dest_Table,Source_view) %}
 {% set tgttable = Dest_Table  %}
 {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %} 
 {% set db_destschema = "'" ~ var('db_factsschema')  ~ "'"  %} 
  
 {{ log("Running  Pre_Load_Steps: " ~ Dest_Table ~ ", " ~ Source_view) }}

    /*Get the SID, LOAD_TYPE, ZONE4_ETL_UPD_DTE from METADATELOADDT table */
    {% set table_query %}
            SELECT LOAD_TYPE, ZONE4_ETL_UPD_DTE, SID from {{var('db_factsschema')}}.{{var('table_metadateloaddt')}} where TGT_TABLENAME ='{{tgttable}}' and MODEL_NAME ='{{Source_view}}';
    {% endset %}

    {% if execute %}
          {%- set vload_type = run_query(table_query).columns[0].values()[0] -%}
          {%- set  vload_type = vload_type|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{ log("Pre_Load_Steps-vload_type: " ~ vload_type) }}

          {%- set vzone4dte = run_query(table_query).columns[1].values()[0] -%}
          {%- set  vzone4dte = vzone4dte|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{ log("Pre_Load_Steps-vzone4dte: " ~ vzone4dte) }}
                
          {%- set vsid = run_query(table_query).columns[2].values()[0] -%}
          {%- set  vsid = vsid|replace("'", '"')|replace("('", '')|replace("',)", '') -%} 
                {{ log("Pre_Load_Steps-vsid: " ~ vsid) }}
                {{ Logging_Ins( Dest_Table,Source_view,vsid)}} 
                                
                {% if vload_type=='FULLR' %}
                                    {{ log("calling Full_Refresh_Steps") }}
                                    {{ Full_Refresh_Steps( Dest_Table,Source_view)}}                                 
                                    
                                    {{ log("Full_Refresh_Steps COMPLETE") }}
                                    
                {% elif vload_type=='SCD1' %}
                                    {{ log("calling first Full_Refresh_Steps for SCD1") }}
                                    {{ Full_Refresh_Steps( Dest_Table,Source_view)}} 
                                    
                                    {{ log("Full Refresh COMPLETE") }}
                {% elif vload_type=='SCD2' %}
                                    {{ log("calling first Full_Refresh_Steps for SCD2") }}
                                    {{ Full_Refresh_Steps_Scd2( Dest_Table,Source_view)}} 
                                    
                                    {{ log("Full Refresh COMPLETE") }}
                {% else %}
                                    {{ Logging_Upd( Dest_Table,Source_view,vsid,'ERROR',3,0,0,'Unknown Load Type: should be either FULLR or SCD1 or SCD2')}}
                                    {{ exceptions.raise_compiler_error("Only FULLR and SCD1 types of data loads are handled") }}       
                        
                {% endif %}
    {% else %}
                
                {{ log("Pre_Load_Steps-failed: ") }}
                {{return(false) }}
    {% endif %}
{{ log("Pre_Load_Steps-SUCCESSFULLY ENDED") }}    
{% endmacro %}
  
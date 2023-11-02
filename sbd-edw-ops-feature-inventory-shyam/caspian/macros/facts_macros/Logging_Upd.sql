{% macro Logging_Upd(Dest_Table,Source_view,Sid_Num,Loadstatus,Loadstep,Logging_Upd,InsCount,UpdCount,Errormsg) %}
/*Loadstep-1 is for complete, 2 is for Error*/
/*InsCount= Contains insert counts when Loadstep=1 else 0 */
/*UpdCount= Contains Update counts when Loadstep=1 else 0 */
/*Errormsg= Contains Error Message when Loadstep=2 else N */

 {% set tgttable = Dest_Table  %}
 {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %} 
 {% set db_destschema = "'" ~ var('db_factsschema')  ~ "'"  %}
 {% set db_srcschema = "'" ~ var('db_srcschema')  ~ "'"  %}
 {% set LoggingTbl = "Logging_"+Source_view+"_tmp" %}
  
 {{ log("Update Log: " ~ LoggingTbl ~ "," ~ Dest_Table ~ ", " ~ Source_view) }}
 {% if Loadstep==1 %}       
        /*{% set Update_Log1 %}
                UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET LOADSTATUS = '{{Loadstatus}}',
                        ROWSINSERTED ={{InsCount}},ROWSUPDATED = {{UpdCount}}
                        WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}';               
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Update_Log1) %}
                {{ log("LOGGING Update completed Loadstep1 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}  */
 
        {% set Update_Log1 %}
                UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET LOADSTATUS = '{{Loadstatus}}', JOB_TIME_END = current_timestamp, 
                        ROWSREAD = {{Logging_Upd}}, ROWSINSERTED ={{InsCount}}, ROWSUPDATED = {{UpdCount}}
                        WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}';               
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Update_Log1) %}
                {{ log("TMP LOGGING Update completed for Loadstep1") }}
        {% else %} 
                {{return(false)}}
        {% endif %}

        {% set Delete_log1 %}
                DELETE FROM {{var('db_factsschema')}}.{{var('table_logging')}} WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}'; 
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Delete_log1) %}
                {{ log("TMP LOGGING Delete completed Loadstep1 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}     

        {% set Insert_log1 %}
                INSERT INTO {{var('db_factsschema')}}.{{var('table_logging')}} SELECT * FROM  {{var('db_srcviewdb')}}.{{LoggingTbl}};
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Insert_log1) %}
                {{ log("LOGGING Insert completed Loadstep1 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}  

 {% else %}

        {% set Update_Log2 %}
                UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET LOADSTATUS = '{{Loadstatus}}', JOB_TIME_END = current_timestamp, 
                        ERROR_DESCRIPTION = '{{Errormsg}}'
                WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}';               
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Update_Log2) %}
                {{ log("TMP LOGGING Update completed Loadstep2 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}

        {% set Delete_log2 %}
                DELETE FROM {{var('db_factsschema')}}.{{var('table_logging')}} WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}'; 
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Delete_log2) %}
                {{ log("TMP LOGGING Delete completed Loadstep2 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}     

        {% set Insert_log2 %}
                INSERT INTO {{var('db_factsschema')}}.{{var('table_logging')}} SELECT * FROM  {{var('db_srcviewdb')}}.{{LoggingTbl}};
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Insert_log2) %}
                {{ log("LOGGING Insert completed Loadstep2 ") }}
        {% else %} 
                {{return(false)}}
        {% endif %}                
        
 {% endif %}

{% endmacro %}
{% macro Logging_Ins(Dest_Table,Source_view,Sid_Num) %}
 {% set tgttable = Dest_Table  %}
 {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %} 
 {% set db_destschema = "'" ~ var('db_factsschema')  ~ "'"  %}
 {% set db_srcschema = "'" ~ var('db_srcschema')  ~ "'"  %}
 {% set LoggingTbl = "Logging_"+Source_view+"_tmp" %}
  
 {{ log("Create First Log: " ~ LoggingTbl ~ "," ~ Dest_Table ~ ", " ~ Source_view) }}
        
        {% set Insert_into_log %}
                INSERT INTO {{var('db_factsschema')}}.{{var('table_logging')}}(LID,SESSION_ID, MODEL_NAME, MODEL_SCHEMA, DSTTBLNAME,DSTSCHEMA,JOB_TIME_START,LOADSTATUS)
                VALUES ({{Sid_Num}},{{get_CurrentSessionID()}},'{{Source_view}}',{{dbtview}},'{{tgttable}}',{{db_destschema}}, current_timestamp,'STARTED');
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Insert_into_log) %}
                {{ log("First Insert into Logging table completed " ~ var('table_logging') ) }}
        {% else %} 
                {{return(false)}}
        {% endif %}   

        {% set Create_tmp_log %}
                CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{LoggingTbl}}
                AS SELECT * FROM {{var('db_factsschema')}}.{{var('table_logging')}} WHERE LID= {{Sid_Num}} and SESSION_ID = {{get_CurrentSessionID()}} and MODEL_NAME ='{{Source_view}}'; 
        {% endset %}

        {% if execute %}
                {%- set result = run_query(Create_tmp_log) %}
                {{ log("Logging table Created " ~ LoggingTbl ) }}
        {% else %} 
                {{return(false)}}
        {% endif %} 
                                      
 {{ log("Logging table inserted successfully") }}         
{% endmacro %}
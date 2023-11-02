/*
      This Macro is used for set incremental macros for source and destination systems, also defined models that needs to execute before and after model.
      Author : Neelima C
      Creation Date : 03/10/2022
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      

      Paramaters :      
      DestTbl : Destination Table
      SourceView : Source_View
*/ 
 
 {% macro Mart_Main_Call(DestTbl,SourceView) %}
  {% set new_query_tag = "VW_" ~ model.name %}
  {% set Dest_Table = DestTbl %}
  {% set Source_view =  SourceView %}
{{ log("Running Main: " ~ DestTbl ~ ", " ~ SourceView) }}
{{ config(materialized='view' ,transient=false,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_pricing_dm',
            pre_hook='{{Pre_Load_Steps ("' ~ Dest_Table ~ '","' ~ Source_view ~ '")}}',
            post_hook='{{Post_Load_Steps ("' ~ Dest_Table ~ '","' ~ Source_view ~ '")}}
                       {{Final_Cleanup ("' ~ Dest_Table ~ '","' ~ Source_view ~ '")}}'      
    )
}}
with source_data as (
    SELECT * FROM {{ref(SourceView)}}
    
    ) 
select *
from source_data

{{ log("Completed Main") }}
{% endmacro %}
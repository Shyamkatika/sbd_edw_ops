{% macro Objects_Release() %}
  {% set new_query_tag = model.name+"_tmp1" %}
    {{ config(materialized='table',transient=true,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_special',
            pre_hook='{{ReleaseObjects()}}')}}
            

    with source_data as (
        Select '1' as number
        ) 
    select *
    from source_data
{% endmacro %}

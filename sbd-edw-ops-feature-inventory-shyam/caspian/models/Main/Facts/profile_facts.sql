{{ config(materialized='ephemeral' ) }}
{{   
profile_sbd_edw_schema(schema_name="FACTS",  object_type="TABLE", limit_records=10 )

}}

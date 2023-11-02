{% macro persist_docs_op(model_name,model,relation = true, columns = true) %}

  {% if execute %}

    {% set query %}  
        select TABLE_NAME from information_schema.TABLES WHERE TABLE_SCHEMA = '{{model_name}}' and table_type = 'BASE TABLE' AND TABLE_NAME = '{{model}}'
    {% endset %}
     {{ log("query" + query, info = true) }}

    {% set tables = run_query(query) %}
    {% set tables_list = tables.columns[0].values()%}
    {{ log("tables_list: " ~ tables_list, True) }}
    {% for table_i in tables_list%}  
        {% set l_check = (graph.nodes.values() | selectattr('name', 'equalto', table_i) | list |length) %}
        {{ log("graph.nodes.values()  : " ~  graph.nodes.values()   , info = false) }}

        {{ log("l_check : " ~  l_check, info = true) }}
        {% if l_check == 1 %}
          {% set model_node = (graph.nodes.values() | selectattr('name', 'equalto', table_i) | list)[0] %}
          {{ log("database : " ~  model_node.database, info = true) }}
          {{ log("schema : " ~  model_node.schema, info = true) }}
          {{ log("alias : " ~  model_node.name, info = true) }}

          {% set relation = adapter.get_relation(
              database=model_node.database, schema=model_node.schema, identifier=model_node.name
          ) %}
          {{ log("relation : " ~  relation, info = true) }}

            {{ log("Altering column comments for " + table_i, info = true) }}
          {{ run_query(sbd__alter_column_comment(relation, model_node.columns)) }}
      {% endif %}
    {%endfor %} 
  {% endif %}

{% endmacro %}

{% macro sbd__alter_column_comment(relation, column_dict) -%}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    alter {{ relation.type }} {{ relation }} alter
    {% for column_name in existing_columns if (column_name in existing_columns) or (column_name|lower in existing_columns) %}
        {{ get_column_comment_sql(column_name, column_dict) }} {{- ',' if not loop.last else ';' }}
    {% endfor %}
{% endmacro %}
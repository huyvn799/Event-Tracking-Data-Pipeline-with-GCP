{% macro generate_created_columns() %}
    current_timestamp() as created_at,
    session_user() as created_by
{% endmacro %}

{% macro generate_updated_columns() %}
    current_timestamp() as updated_at,
    session_user() as updated_by
{% endmacro %}
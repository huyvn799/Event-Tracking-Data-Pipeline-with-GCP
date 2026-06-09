{% macro generate_created_coulmns() %}
    current_timestamp() as created_at,
    session_user() as created_by
{% endmacro %}

{% macro generate_updated_coulmns() %}
    current_timestamp() as updated_at,
    session_user() as updated_by
{% endmacro %}
{% macro get_updated_value(condition_to_be_updated, new_value, old_value) %}
    case
        when {{ condition_to_be_updated }} then {{ new_value }}
        else {{ old_value }}
    end
{% endmacro %}
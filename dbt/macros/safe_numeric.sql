{% macro safe_numeric(column_name) -%}
    case
        when {{ column_name }} ~ '^-?\d+(\.\d+)?$'
        then {{ column_name }}::numeric
    end
{%- endmacro %}
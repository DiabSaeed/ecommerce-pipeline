{%- macro generate_schema_name (custom_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_name | trim }}
    {%- endif -%}
{%- endmacro -%}
{% macro validated_sha256(value) -%}
  {%- if not modules.re.fullmatch('[0-9a-f]{64}', value | string) -%}
    {{ exceptions.raise_compiler_error('Expected a lowercase SHA-256 reference') }}
  {%- endif -%}
  {{- value -}}
{%- endmacro %}

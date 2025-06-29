{% test expression_is(model, expression, column_name=None) %}
    select *
    from {{ model }}
    where not ({{ expression }})
{% endtest %}
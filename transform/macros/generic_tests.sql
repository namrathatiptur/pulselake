{#
    Custom generic tests.

    These are written by hand rather than pulled in from dbt_utils. The
    package would work fine, but for two small tests it would add a
    `dbt deps` step and a network download to a project whose whole point is
    that it runs locally with no external setup. Writing the test also makes
    it obvious how a dbt test actually works: it is just a query, and any row
    it returns is a failure.
#}


{% test positive_value(model, column_name) %}
    -- Fails if the column is zero, negative, or null.
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is null
       or {{ column_name }} <= 0
{% endtest %}


{% test at_most(model, column_name, max_value) %}
    -- Fails if any value exceeds the given ceiling. Used for sanity bounds
    -- where an exact expectation would be too brittle.
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} > {{ max_value }}
{% endtest %}

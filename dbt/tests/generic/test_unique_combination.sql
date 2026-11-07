{% test unique_combination(model, columns) %}

-- Fails if any combination of the given columns appears more than once
select
    {{ columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
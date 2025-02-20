#####
WHERE
#####

The query languages allows defining simple filters as well as existential filters. A simple filter is a filter that compares
a single field to a constant expression, or a constant expression to another (in that case, the expression is evaluated immediately
and actual query execution will not happen if, e.g., the predicate evaluates to :sql:`false`).

Syntax
######

.. raw:: html
    :file: WHERE.diagram.svg

Parameter
#########

* :sql:`booleanExpression`
    A Boolean expression. If :sql:`true`, the row should be included. If :sql:`false` or :sql:`NULL`, the row should not be included.

Examples
########

Single predicate
----------------

Suppose that we want to filter all restaurants from the example above whose :sql:`rest_no` is larger or equal to 44:

.. code-block:: sql

    select rest_no, name from restaurant where rest_no >= 44;

will return:

.. list-table::
    :header-rows: 1

    * - :sql:`rest_no`
      - :sql:`name`
    * - 44
      - Restaurant3
    * - 45
      - Restaurant4

Multiple predicates
-------------------

It is also possible to have a conjunction of predicates, say we want to view restaurants whose :sql:`rest_no` is between 43 and 45

.. code-block:: sql

    select rest_no, name from restaurant where rest_no > 43 and rest_no < 45;

will return

.. list-table::
    :header-rows: 1

    * - :sql:`rest_no`
      - :sql:`name`
    * - 44
      - Restaurant3

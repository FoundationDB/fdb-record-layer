=======
EXPLAIN
=======

You can prepend any :sql:`SELECT` statement with the keyword :sql:`EXPLAIN` to display the physical plan
that the engine would use to execute it. No rows are read from the database.

Syntax
######

.. raw:: html
    :file: EXPLAIN.diagram.svg


Parameters
##########

``query``
    The :sql:`SELECT` statement to explain.

``column``
    An optional comma-separated list of output columns to return, enclosed in parentheses.
    Valid column names are: :sql:`PLAN`, :sql:`PLAN_HASH`, :sql:`PLAN_DOT`, :sql:`PLAN_GML`,
    :sql:`PLAN_CONTINUATION`, :sql:`PLANNER_METRICS`.

    When a column list is provided the result set contains only those columns, and expensive
    graph computations (:sql:`PLAN_DOT`, :sql:`PLAN_GML`) are skipped when neither is requested.
    Columns are always returned in the fixed order listed above, regardless of the order they
    appear in the column list.

    When no column list is provided, all six columns are returned.

Returns
#######

The result set contains one row. The columns present depend on whether a column list was specified
(see ``column`` above). The possible columns, in their fixed return order, are:

* :sql:`PLAN`
    A textual representation of the physical query plan.
* :sql:`PLAN_HASH`
    A hash that allows quick disambiguation between two plans.
* :sql:`PLAN_DOT`
    A DOT-language representation of the plan graph, suitable for rendering with Graphviz.
* :sql:`PLAN_GML`
    A Graph Modelling Language (GML) representation of the plan graph.
* :sql:`PLAN_CONTINUATION`
    A byte array containing the serialized plan, used for plan caching and continuations.
* :sql:`PLANNER_METRICS`
    A nested record containing planner performance metrics collected during plan generation.

Examples
########

Full explain
************

.. code-block:: sql

    EXPLAIN SELECT name FROM restaurant WHERE rest_no >= 44;

Returns all six columns:

.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50 25

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
      - :sql:`PLANNER_METRICS`
    * - .. code-block::

            SCAN(<,>)
            | TFILTER RestaurantComplexRecord
            | FILTER _.rest_no GREATER_THAN_OR_EQUALS 44
            | MAP (_.name AS name)
      - :sql:`-1635569052`
      - dot_graph
      - gml_graph
      - continuation
      - metrics

Selecting specific columns
**************************

When only certain columns are needed, list them in parentheses after :sql:`EXPLAIN`.
This avoids computing the DOT and GML graph representations when they are not requested,
which can be significant for large plans.

.. code-block:: sql

    EXPLAIN (PLAN, PLAN_HASH) SELECT name FROM restaurant WHERE rest_no >= 44;

Returns only the two requested columns:

.. list-table::
    :header-rows: 1
    :widths: 50 25

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
    * - .. code-block::

            SCAN(<,>)
            | TFILTER RestaurantComplexRecord
            | FILTER _.rest_no GREATER_THAN_OR_EQUALS 44
            | MAP (_.name AS name)
      - :sql:`-1635569052`

Index scan
**********

.. code-block:: sql

    EXPLAIN
        SELECT name
        FROM RestaurantComplexRecord USE INDEX (record_name_idx)
        WHERE rest_no > 10


.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50 25

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
      - :sql:`PLANNER_METRICS`
    * - .. code-block::

            COVERING(record_name_idx <,> -> [name: KEY[1], rest_no: KEY[2]])
            | FILTER _.rest_no GREATER_THAN 10
            | MAP (_.name AS name)
      - :sql:`-1543467542`
      - dot_graph
      - gml_graph
      - continuation
      - metrics


.. code-block:: sql

    EXPLAIN
        SELECT *
        FROM RestaurantComplexRecord AS R
        WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)

.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50 25

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
      - :sql:`PLANNER_METRICS`
    * - .. code-block::

            ISCAN(mv1 [[9],>)
            | MAP (_.0 AS rest_no, _.1 AS name, _.2 AS location, _.3 AS reviews, _.4 AS tags, _.5 AS customer, _.6 AS encoded_bytes)
      - :sql:`-8677659052`
      - dot_graph
      - gml_graph
      - continuation
      - metrics

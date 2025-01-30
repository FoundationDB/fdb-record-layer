=======
EXPLAIN
=======

You can prepend any :sql:`SELECT` statement with the keyword :sql:`EXPLAIN` to display the physical plan that is used during the execution.

Syntax
######

.. raw:: html
    :file: EXPLAIN.diagram.svg


Parameters
##########

``query``
    The query to explain.

Returns
#######

* :sql:`PLAN`
    A textual representation of the plan
* :sql:`PLAN_HASH`
    A hash that allows to quickly disambiguate between two plans
* :sql:`PLAN_DOT`
    A dot representation of the plan
* :sql:`PLAN_GML`
    A Graph Modelling Language (GML) representation of the plan
* :sql:`PLAN_CONTINUATION`
    A byte array that contains information that is essential in the context of plan serialization

Examples
########

TODO change examples to render dot graph and put actual plan hash

Table scan
**********

.. code-block:: sql

    EXPLAIN SELECT name FROM restaurant WHERE rest_no >= 44;

.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
    * - .. code-block::

            SCAN(<,>)
            | TFILTER RestaurantComplexRecord
            | FILTER _.rest_no GREATER_THAN_OR_EQUALS 42
            | MAP (_.name AS name)
      - :sql:`-1635569052`
      - dot_graph
      - gml_graph
      - continuation


Index scan
**********

.. code-block:: sql

    EXPLAIN
        SELECT name
        FROM RestaurantComplexRecord USE INDEX (record_name_idx)
        WHERE rest_no > 10


.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
    * - .. code-block::

            COVERING(record_name_idx <,> -> [name: KEY[1], rest_no: KEY[2]])
            | FILTER _.rest_no GREATER_THAN 10
            | MAP (_.name AS name)
      - :sql:`-1543467542`
      - dot_graph
      - gml_graph
      - continuation


.. code-block:: sql

    EXPLAIN
        SELECT *
        FROM RestaurantComplexRecord AS R
        WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)

.. list-table::
    :header-rows: 1
    :widths: 50 25 50 50 50

    * - :sql:`PLAN`
      - :sql:`PLAN_HASH`
      - :sql:`PLAN_DOT`
      - :sql:`PLAN_GML`
      - :sql:`PLAN_CONTINUATION`
    * - .. code-block::

            ISCAN(mv1 [[9],>)
            | MAP (_.0 AS rest_no, _.1 AS name, _.2 AS location, _.3 AS reviews, _.4 AS tags, _.5 AS customer, _.6 AS encoded_bytes)
      - :sql:`-8677659052`
      - dot_graph
      - gml_graph
      - continuation

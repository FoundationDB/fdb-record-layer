===
SUM
===

.. _sum:

Computes the sum of all non-NULL values in a group.

Syntax
======

.. raw:: html
    :file: sum.diagram.svg

Parameters
==========

``SUM(expression)``
    Sums all non-NULL values of ``expression`` in the group. NULL values are ignored.

Returns
=======

Returns a ``BIGINT`` representing the sum of all non-NULL values. If all values are NULL or the input set is empty, returns NULL.

Examples
========

Setup
-----

For these examples, assume we have a ``sales`` table:

.. code-block:: sql

    CREATE TABLE sales(
        id BIGINT,
        product STRING,
        region STRING,
        amount BIGINT,
        PRIMARY KEY(id))

    INSERT INTO sales VALUES
        (1, 'Widget', 'North', 100),
        (2, 'Widget', 'South', 150),
        (3, 'Gadget', 'North', 200),
        (4, 'Gadget', 'South', NULL),
        (5, 'Widget', 'North', 120)

SUM - Sum All Values
---------------------

Sum all amounts in the table:

.. code-block:: sql

    SELECT SUM(amount) AS total_amount FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`total_amount`
    * - :json:`570`

Notice that the NULL value in row 4 is ignored in the sum.

SUM with GROUP BY
------------------

Sum amounts per product:

.. code-block:: sql

    SELECT product, SUM(amount) AS total_amount
    FROM sales
    GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`total_amount`
    * - :json:`"Widget"`
      - :json:`370`
    * - :json:`"Gadget"`
      - :json:`200`

Sum amounts per region:

.. code-block:: sql

    SELECT region, SUM(amount) AS total_amount
    FROM sales
    GROUP BY region

.. list-table::
    :header-rows: 1

    * - :sql:`region`
      - :sql:`total_amount`
    * - :json:`"North"`
      - :json:`420`
    * - :json:`"South"`
      - :json:`150`

The South region sum only includes the non-NULL value (150), ignoring the NULL from the Gadget sale.

Important Notes
===============

* ``SUM`` ignores NULL values in the aggregation
* When used without GROUP BY, SUM returns a single value for the entire table
* When used with GROUP BY, SUM returns one value per group
* If all values in a group are NULL, SUM returns NULL for that group
* The return type is ``BIGINT``
* **Index Requirement**: For optimal performance, queries with GROUP BY require an appropriate index. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

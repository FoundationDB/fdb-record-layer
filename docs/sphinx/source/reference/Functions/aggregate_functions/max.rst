===
MAX
===

.. _max:

Returns the maximum value from all non-NULL values in a group.

Syntax
======

.. raw:: html
    :file: max.diagram.svg

Parameters
==========

``MAX(expression)``
    Returns the largest non-NULL value of ``expression`` in the group. NULL values are ignored.

Returns
=======

Returns the maximum value with the same type as ``expression``. If all values are NULL or the input set is empty, returns NULL.

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

MAX - Maximum Value
--------------------

Find the maximum amount in the table:

.. code-block:: sql

    SELECT MAX(amount) AS max_amount FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`max_amount`
    * - :json:`200`

Notice that the NULL value in row 4 is ignored.

MAX with GROUP BY
------------------

Find maximum amounts per product:

.. code-block:: sql

    SELECT product, MAX(amount) AS max_amount
    FROM sales
    GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`max_amount`
    * - :json:`"Widget"`
      - :json:`150`
    * - :json:`"Gadget"`
      - :json:`200`

Find maximum amounts per region:

.. code-block:: sql

    SELECT region, MAX(amount) AS max_amount
    FROM sales
    GROUP BY region

.. list-table::
    :header-rows: 1

    * - :sql:`region`
      - :sql:`max_amount`
    * - :json:`"North"`
      - :json:`200`
    * - :json:`"South"`
      - :json:`150`

The South region maximum only considers the non-NULL value (150), ignoring the NULL from the Gadget sale.

Important Notes
===============

* ``MAX`` ignores NULL values in the aggregation
* When used without GROUP BY, MAX returns a single value for the entire table
* When used with GROUP BY, MAX returns one value per group
* If all values in a group are NULL, MAX returns NULL for that group
* The return type matches the type of the input expression
* ``MAX`` can be used with numeric types, strings (lexicographic ordering), and other comparable types
* **Index Requirement**: For optimal performance, queries with GROUP BY require an appropriate index. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

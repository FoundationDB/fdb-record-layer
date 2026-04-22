===
MIN
===

.. _min:

Returns the minimum value from all non-NULL values in a group.

Syntax
======

.. raw:: html
    :file: min.diagram.svg

Parameters
==========

``MIN(expression)``
    Returns the smallest non-NULL value of ``expression`` in the group. NULL values are ignored.

Returns
=======

Returns the minimum value with the same type as ``expression``. If all values are NULL or the input set is empty, returns NULL.

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

MIN - Minimum Value
--------------------

Find the minimum amount in the table:

.. code-block:: sql

    SELECT MIN(amount) AS min_amount FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`min_amount`
    * - :json:`100`

Notice that the NULL value in row 4 is ignored.

MIN with GROUP BY
------------------

Find minimum amounts per product:

.. code-block:: sql

    SELECT product, MIN(amount) AS min_amount
    FROM sales
    GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`min_amount`
    * - :json:`"Widget"`
      - :json:`100`
    * - :json:`"Gadget"`
      - :json:`200`

Find minimum amounts per region:

.. code-block:: sql

    SELECT region, MIN(amount) AS min_amount
    FROM sales
    GROUP BY region

.. list-table::
    :header-rows: 1

    * - :sql:`region`
      - :sql:`min_amount`
    * - :json:`"North"`
      - :json:`100`
    * - :json:`"South"`
      - :json:`150`

The South region minimum only considers the non-NULL value (150), ignoring the NULL from the Gadget sale.

Important Notes
===============

* ``MIN`` ignores NULL values in the aggregation
* When used without GROUP BY, MIN returns a single value for the entire table
* When used with GROUP BY, MIN returns one value per group
* If all values in a group are NULL, MIN returns NULL for that group
* The return type matches the type of the input expression
* ``MIN`` can be used with numeric types, strings (lexicographic ordering), and other comparable types
* **Index Requirement**: For optimal performance, queries with GROUP BY require an appropriate index. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

=====
COUNT
=====

.. _count:

Counts the number of rows or non-NULL values in a group.

Syntax
======

.. raw:: html
    :file: count.diagram.svg

Parameters
==========

The function accepts two forms:

``COUNT(*)``
    Counts all rows in the group, including rows with NULL values.

``COUNT(expression)``
    Counts only the rows where ``expression`` is not NULL.

Returns
=======

Returns a ``BIGINT`` representing the count of rows or non-NULL values. If the input set is empty, returns ``0``.

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

COUNT(*) - Count All Rows
--------------------------

Count all rows in the table:

.. code-block:: sql

    SELECT COUNT(*) AS total_sales FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`total_sales`
    * - :json:`5`

COUNT(column) - Count Non-NULL Values
--------------------------------------

Count only non-NULL amounts:

.. code-block:: sql

    SELECT COUNT(amount) AS sales_with_amount FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`sales_with_amount`
    * - :json:`4`

Notice that the count is 4, not 5, because the fourth row has a NULL ``amount``.

COUNT with GROUP BY
-------------------

Count sales per product:

.. code-block:: sql

    SELECT product, COUNT(*) AS sales_count
    FROM sales
    GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`sales_count`
    * - :json:`"Widget"`
      - :json:`3`
    * - :json:`"Gadget"`
      - :json:`2`

Count non-NULL amounts per region:

.. code-block:: sql

    SELECT region, COUNT(amount) AS non_null_amounts
    FROM sales
    GROUP BY region

.. list-table::
    :header-rows: 1

    * - :sql:`region`
      - :sql:`non_null_amounts`
    * - :json:`"North"`
      - :json:`3`
    * - :json:`"South"`
      - :json:`1`

The South region has 2 sales, but only 1 has a non-NULL amount.

Important Notes
===============

* ``COUNT(*)`` counts all rows, including those with NULL values in any column
* ``COUNT(column)`` counts only rows where the specified column is not NULL
* When used without GROUP BY, COUNT returns a single value for the entire table
* When used with GROUP BY, COUNT returns one value per group
* The return type is always ``BIGINT``
* **Index Requirement**: For optimal performance, queries with GROUP BY require an appropriate index. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

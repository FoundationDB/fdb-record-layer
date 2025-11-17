===
AVG
===

.. _avg:

Computes the average (arithmetic mean) of all non-NULL values in a group.

Syntax
======

.. raw:: html
    :file: avg.diagram.svg

Parameters
==========

``AVG(expression)``
    Calculates the average of all non-NULL values of ``expression`` in the group. NULL values are ignored.

Returns
=======

Returns a ``DOUBLE`` representing the average of all non-NULL values. If all values are NULL or the input set is empty, returns NULL.

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

AVG - Average All Values
-------------------------

Calculate the average of all amounts in the table:

.. code-block:: sql

    SELECT AVG(amount) AS average_amount FROM sales

.. list-table::
    :header-rows: 1

    * - :sql:`average_amount`
    * - :json:`142.5`

Notice that the NULL value in row 4 is ignored, so the average is 570 / 4 = 142.5.

AVG with GROUP BY
------------------

Calculate average amounts per product:

.. code-block:: sql

    SELECT product, AVG(amount) AS average_amount
    FROM sales
    GROUP BY product

.. list-table::
    :header-rows: 1

    * - :sql:`product`
      - :sql:`average_amount`
    * - :json:`"Widget"`
      - :json:`123.33333333333333`
    * - :json:`"Gadget"`
      - :json:`200.0`

Calculate average amounts per region:

.. code-block:: sql

    SELECT region, AVG(amount) AS average_amount
    FROM sales
    GROUP BY region

.. list-table::
    :header-rows: 1

    * - :sql:`region`
      - :sql:`average_amount`
    * - :json:`"North"`
      - :json:`140.0`
    * - :json:`"South"`
      - :json:`150.0`

The South region average only includes the non-NULL value (150), ignoring the NULL from the Gadget sale.

Important Notes
===============

* ``AVG`` ignores NULL values in the aggregation
* When used without GROUP BY, AVG returns a single value for the entire table
* When used with GROUP BY, AVG returns one value per group
* If all values in a group are NULL, AVG returns NULL for that group
* The return type is ``DOUBLE`` (even if the input is ``BIGINT``)
* **Index Requirement**: For optimal performance, queries with GROUP BY require an appropriate index. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

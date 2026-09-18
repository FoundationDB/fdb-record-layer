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

* **Index Requirement**: GROUP BY queries require an appropriate index to execute. See :ref:`Indexes <index_definition>` for details on creating indexes that support GROUP BY operations.

====
LEAST
=====

.. _least:

Returns the least (minimum) value from a list of expressions.

Syntax
======

.. raw:: html
    :file: least.diagram.svg

Parameters
==========

``LEAST(expression1, expression2, ...)``
    Returns the smallest value among all provided expressions. Requires at least two expressions. NULL values are considered smaller than any non-NULL value.

Returns
=======

Returns the least value with the same type as the input expressions. If any value is NULL, returns NULL. All expressions must be of compatible types.

Supported Types
================

``LEAST`` supports the following types:

* ``STRING`` - Lexicographic comparison
* ``BOOLEAN`` - FALSE < TRUE
* ``DOUBLE`` - Numeric comparison
* ``FLOAT`` - Numeric comparison
* ``INTEGER`` - Numeric comparison
* ``BIGINT`` - Numeric comparison

**Not Supported**: ``ARRAY``, ``STRUCT``, ``BYTES``

Examples
========

Setup
-----

For these examples, assume we have a ``products`` table:

.. code-block:: sql

    CREATE TABLE products(
        id BIGINT,
        name STRING,
        price_usd BIGINT,
        price_eur BIGINT,
        price_gbp BIGINT,
        PRIMARY KEY(id))

    INSERT INTO products VALUES
        (1, 'Widget', 100, 90, 80),
        (2, 'Gadget', 150, 140, 120),
        (3, 'Doohickey', 200, 180, 160)

LEAST - Find Minimum Price
----------------------------

Find the lowest price across all currencies for each product:

.. code-block:: sql

    SELECT name, LEAST(price_usd, price_eur, price_gbp) AS min_price
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`min_price`
    * - :json:`"Widget"`
      - :json:`80`
    * - :json:`"Gadget"`
      - :json:`120`
    * - :json:`"Doohickey"`
      - :json:`160`

LEAST with Constants
---------------------

Compare values with constants:

.. code-block:: sql

    SELECT name, LEAST(price_usd, 175) AS capped_price
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`capped_price`
    * - :json:`"Widget"`
      - :json:`100`
    * - :json:`"Gadget"`
      - :json:`150`
    * - :json:`"Doohickey"`
      - :json:`175`

This ensures a maximum price cap of 175 for all products.

Important Notes
===============

* ``LEAST`` returns the smallest value among all provided expressions
* If any value is NULL, ``LEAST`` returns NULL
* All expressions must be of compatible types
* The function requires at least two arguments
* For string comparisons, lexicographic ordering is used

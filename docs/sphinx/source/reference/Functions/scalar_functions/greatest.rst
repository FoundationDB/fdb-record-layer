========
GREATEST
========

.. _greatest:

Returns the greatest (maximum) value from a list of expressions.

Syntax
======

.. raw:: html
    :file: greatest.diagram.svg

Parameters
==========

``GREATEST(expression1, expression2, ...)``
    Returns the largest value among all provided expressions. Requires at least two expressions. NULL values are considered smaller than any non-NULL value.

Returns
=======

Returns the greatest value with the same type as the input expressions. If all values are NULL, returns NULL. All expressions must be of compatible types.

Supported Types
================

``GREATEST`` supports the following types:

* ``STRING`` - Lexicographic comparison
* ``BOOLEAN`` - TRUE > FALSE
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

GREATEST - Find Maximum Price
-------------------------------

Find the highest price across all currencies for each product:

.. code-block:: sql

    SELECT name, GREATEST(price_usd, price_eur, price_gbp) AS max_price
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`max_price`
    * - :json:`"Widget"`
      - :json:`100`
    * - :json:`"Gadget"`
      - :json:`150`
    * - :json:`"Doohickey"`
      - :json:`200`

GREATEST with Constants
-------------------------

Compare values with constants:

.. code-block:: sql

    SELECT name, GREATEST(price_usd, 125) AS adjusted_price
    FROM products

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`adjusted_price`
    * - :json:`"Widget"`
      - :json:`125`
    * - :json:`"Gadget"`
      - :json:`150`
    * - :json:`"Doohickey"`
      - :json:`200`

This ensures a minimum price of 125 for all products.

Important Notes
===============

* ``GREATEST`` returns the largest value among all provided expressions
* NULL values are treated as smaller than any non-NULL value
* If all values are NULL, ``GREATEST`` returns NULL
* All expressions must be of compatible types
* The function requires at least two arguments
* For string comparisons, lexicographic ordering is used

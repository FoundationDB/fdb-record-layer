====================
Comparison Operators
====================

.. _comparison-operators:

Standard Comparison Operators
==============================

We support standard comparison operators: ``>``, ``>=``, ``=``, ``<``, ``<=``, ``!=``, ``<>``

If either the left or right operand is NULL, the comparison result is NULL.

.. code-block:: sql

    SELECT * FROM products WHERE price > 100
    SELECT * FROM products WHERE price <= 50
    SELECT * FROM products WHERE name = 'Widget'
    SELECT * FROM products WHERE stock != 0

IS DISTINCT FROM
================

NULL-safe comparison operator that treats NULL values as comparable:

- ``expression1 IS DISTINCT FROM expression2`` - Returns ``TRUE`` if values are different (treating NULL as a value)
- ``expression1 IS NOT DISTINCT FROM expression2`` - Returns ``TRUE`` if values are the same (treating NULL as a value)

Unlike ``=`` and ``!=``, this operator never returns NULL.

For detailed documentation, examples, and use cases, see :ref:`IS DISTINCT FROM <is-distinct-from>`.

See Also
========

* :ref:`IS Operator <is-operators>` - IS NULL, IS TRUE, IS FALSE

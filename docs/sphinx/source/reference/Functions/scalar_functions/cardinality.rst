===========
CARDINALITY
===========

The CARDINALITY function returns the cardinality of an array.

Syntax
======

.. code-block:: sql

    CARDINALITY( <expr> )

Parameters
==========

``expr``
    An array expression.

Returns
=======

Given an array value, returns a value of type INTEGER representing the number of elements in the array, or 0 if it is
empty. If the argument is NULL, the result is the NULL value.

Example
=======

.. code-block:: sql

    CREATE TABLE t1 (id INTEGER, arr INTEGER ARRAY);
    INSERT INTO t1 VALUES (1, [1, 2]);
    SELECT CARDINALITY(arr) FROM t1;  -- yields 2

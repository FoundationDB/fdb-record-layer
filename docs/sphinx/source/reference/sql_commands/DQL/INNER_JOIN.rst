==========
INNER JOIN
==========

.. _inner_join:

Combines rows from two sources based on a join condition. INNER JOIN returns only the rows where the join condition is satisfied in both tables.

Syntax
======

.. raw:: html
    :file: INNER_JOIN.diagram.svg

The INNER JOIN clause is used in SELECT statements:

.. code-block:: sql

    SELECT columns
    FROM table1 INNER JOIN table2
    ON table1.column = table2.column

The ``JOIN`` keyword can be used as a shorthand for ``INNER JOIN``:

Parameters
==========

``source``
    The table or subquery to join with.

``ON joinCondition``
    A boolean expression that specifies the join condition. Only rows where this condition evaluates to true are included in the result.

``USING (column(s))``
    A comma-separated list of column names that exist in both tables. This is shorthand for joining on equality of these columns. The USING clause automatically removes duplicate columns from the result set.

Returns
=======

Returns a result set containing rows where the join condition is satisfied. For each matching pair of rows from the two sources, a combined row is produced with columns from both tables.

For more details see :doc:`../../Joins`.

See Also
========

* :doc:`SELECT` - SELECT statement syntax
* :doc:`WHERE` - WHERE clause filtering
* :doc:`../../Subqueries` - Subqueries and correlated subqueries
* :doc:`WITH` - Common Table Expressions

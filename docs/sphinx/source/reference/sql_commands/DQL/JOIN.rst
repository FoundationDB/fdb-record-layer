====
JOIN
====

.. _inner_join:
.. _left_outer_join:
.. _right_outer_join:
.. _outer_join:

The FDB Record Layer supports ``INNER JOIN``, ``LEFT OUTER JOIN``, and ``RIGHT OUTER JOIN``. All three accept an ``ON`` condition or a ``USING`` column list. The ``FULL OUTER JOIN`` variant is not yet supported.

INNER JOIN
==========

An ``INNER JOIN`` combines rows from two sources based on a join condition. It returns only the rows where the join condition is satisfied in both tables.

Syntax
------

.. raw:: html
    :file: INNER_JOIN.diagram.svg

.. code-block:: sql

    SELECT columns
    FROM table1 INNER JOIN table2
    ON table1.column = table2.column

The ``INNER`` keyword is optional: ``JOIN`` can be used as a shorthand for ``INNER JOIN``.

Parameters
----------

``source``
    The table or subquery to join with.

``ON joinCondition``
    A boolean expression that specifies the join condition. Only rows where this condition evaluates to true are included in the result.

``USING (column(s))``
    A comma-separated list of column names that exist in both tables. This is shorthand for joining on equality of these columns. The ``USING`` clause automatically removes duplicate columns from the result set.

Returns
-------

Returns a result set containing rows where the join condition is satisfied. For each matching pair of rows from the two sources, a combined row is produced with columns from both tables.

OUTER JOIN
==========

An ``OUTER JOIN`` combines rows from two sources based on a join condition. Unlike an inner join, an outer join preserves every row from one of the two sides. When no matching row exists on the other side, the columns from that side are filled with ``NULL``.

The FDB Record Layer supports ``LEFT OUTER JOIN`` and ``RIGHT OUTER JOIN``. A ``LEFT OUTER JOIN`` preserves every row from the left side; a ``RIGHT OUTER JOIN`` preserves every row from the right side. Aside from the order of the produced columns, the two are equivalent: ``A LEFT OUTER JOIN B ON …`` produces the same set of rows as ``B RIGHT OUTER JOIN A ON …``.

Syntax
------

.. raw:: html
    :file: OUTER_JOIN.diagram.svg

.. code-block:: sql

    SELECT columns
    FROM table1 LEFT OUTER JOIN table2
    ON table1.column = table2.column

    SELECT columns
    FROM table1 RIGHT OUTER JOIN table2
    ON table1.column = table2.column

The ``OUTER`` keyword is optional: ``LEFT JOIN`` and ``LEFT OUTER JOIN`` are equivalent, as are ``RIGHT JOIN`` and ``RIGHT OUTER JOIN``.

Parameters
----------

``source``
    The table or subquery to join with.

``ON joinCondition``
    A boolean expression that specifies the join condition. Rows on the preserved side (left for ``LEFT OUTER JOIN``, right for ``RIGHT OUTER JOIN``) that have no match on the other side produce a single output row with ``NULL`` in every column from the other side.

``USING (column(s))``
    A comma-separated list of column names that exist in both tables. This is shorthand for joining on equality of these columns. As with ``INNER JOIN … USING``, duplicate columns are hidden from the result set.

Returns
-------

Returns a result set containing:

- One combined row for every matching pair (same as an inner join).
- One null-padded row for every row on the preserved side that has *no* match on the other side.

Every row from the preserved side therefore appears at least once in the output.

Examples
========

For the examples below, assume the following tables:

.. code-block:: sql

    CREATE TABLE emp (
        id BIGINT, fname STRING, lname STRING, dept_id BIGINT,
        PRIMARY KEY(id)
    )
    CREATE TABLE dept (
        id BIGINT, name STRING,
        PRIMARY KEY(id)
    )

    INSERT INTO emp VALUES
        (1, 'Alice', 'Smith', 1),
        (2, 'Bob',   'Jones', 1),
        (3, 'Carol', 'Lee',   2),
        (4, 'Dave',  'Kim',  99)

    INSERT INTO dept VALUES
        (1, 'Engineering'),
        (2, 'Sales'),
        (3, 'Marketing')

Basic LEFT OUTER JOIN
---------------------

Return every employee together with their department. Dave (``dept_id = 99``) has no matching department, so the department name in the result is ``NULL``.

.. code-block:: sql

    SELECT e.fname, e.lname, d.name
    FROM emp e LEFT OUTER JOIN dept d ON e.dept_id = d.id

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
      - :sql:`name`
    * - :json:`"Alice"`
      - :json:`"Smith"`
      - :json:`"Engineering"`
    * - :json:`"Bob"`
      - :json:`"Jones"`
      - :json:`"Engineering"`
    * - :json:`"Carol"`
      - :json:`"Lee"`
      - :json:`"Sales"`
    * - :json:`"Dave"`
      - :json:`"Kim"`
      - ``NULL``

RIGHT OUTER JOIN
----------------

Because ``A LEFT OUTER JOIN B`` and ``B RIGHT OUTER JOIN A`` produce the same set of rows, the query above can equivalently be written as:

.. code-block:: sql

    SELECT e.fname, e.lname, d.name
    FROM dept d RIGHT OUTER JOIN emp e ON e.dept_id = d.id

Use whichever direction reads more naturally for the query at hand.

Anti-Join Pattern
-----------------

Use ``LEFT OUTER JOIN`` with ``WHERE … IS NULL`` on the right side to find only the rows with *no* match. This is a common way to express an anti-join in SQL.

.. code-block:: sql

    SELECT e.fname, e.lname
    FROM emp e LEFT JOIN dept d ON e.dept_id = d.id
    WHERE d.id IS NULL

.. list-table::
    :header-rows: 1

    * - :sql:`fname`
      - :sql:`lname`
    * - :json:`"Dave"`
      - :json:`"Kim"`

Chained Joins
-------------

A ``LEFT OUTER JOIN`` can follow an ``INNER JOIN`` or another ``LEFT OUTER JOIN``:

.. code-block:: sql

    SELECT e.fname, d.name, p.name
    FROM emp e
    JOIN dept d ON e.dept_id = d.id
    LEFT JOIN project p ON e.id = p.emp_id

For further examples, see :doc:`../../Joins`.

See Also
========

* :doc:`SELECT` - SELECT statement syntax
* :doc:`WHERE` - WHERE clause filtering
* :doc:`../../Subqueries` - Subqueries and correlated subqueries
* :doc:`WITH` - Common Table Expressions
* :doc:`../../Joins` - General overview of joins, with examples

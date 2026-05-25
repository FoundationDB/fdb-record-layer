=========
SET LOCAL
=========

.. _set_local:

Binds a named value to the current transaction. The value can then be referenced in any SQL
statement executed within that transaction using the ``@name`` syntax.

Syntax
======

.. code-block:: sql

    SET LOCAL name = constant

``@name`` reference syntax:

.. code-block:: sql

    SELECT ... WHERE column = @name

Parameters
==========

``name``
    The variable name. Case-insensitive. Must be a valid SQL identifier.

``constant``
    A literal constant value. Expressions are not supported. The type is inferred from the
    literal (e.g. ``42`` is ``BIGINT``, ``'text'`` is ``STRING``).

Lifecycle
=========

Variables are scoped to the current transaction:

- A variable set with ``SET LOCAL`` is visible to all statements in the same transaction.
- When the transaction commits or is aborted, all local variables are discarded.
- Setting the same variable twice overwrites the previous value.

Using ``@name`` in queries
==========================

``@name`` can appear anywhere a column reference or literal is valid: ``WHERE`` clauses,
``SELECT`` lists, function arguments, subqueries, and function bodies.

.. code-block:: sql

    SET LOCAL min_salary = 100000

    SELECT id, name, salary
    FROM employees
    WHERE salary >= @min_salary

Using variables in function bodies
===================================

Both permanent and temporary functions can reference ``@name`` variables. The variable is
resolved at query execution time from the calling transaction's local variable state.

.. code-block:: sql

    CREATE FUNCTION high_earners(IN dept STRING)
    AS SELECT id, name, salary
       FROM employees
       WHERE department = dept AND salary >= @min_salary

    SET LOCAL min_salary = 110000

    SELECT * FROM high_earners('Engineering')

The function body sees ``@min_salary`` from the transaction that called it, not from the
transaction that created the function.

Independence from ``?param``
=============================

``@name`` (local variable) and ``?name`` (named prepared statement parameter) are independent
namespaces. A query may use both ``@x`` and ``?x`` with the same name; they resolve to
different values.

.. code-block:: sql

    SET LOCAL x = 1

    -- @x resolves to 1 (local variable); ?x is supplied separately as a prepared parameter
    SELECT pk FROM tbl WHERE pk = @x OR pk = ?x

Continuations
=============

When a query is executed, the current value of each referenced ``@name`` variable is captured
and baked into the continuation bytes. If the variable is later changed with ``SET LOCAL`` and
the continuation is resumed, the original captured value is used — not the new one.

.. code-block:: sql

    SET LOCAL min_pk = 2
    -- Execute query, get page 1 continuation

    SET LOCAL min_pk = 99
    -- Resuming the continuation still uses min_pk = 2

Examples
========

Setup
-----

.. code-block:: sql

    CREATE TABLE employees(
        id BIGINT,
        name STRING,
        department STRING,
        salary BIGINT,
        PRIMARY KEY(id))

    INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 100000),
        (2, 'Bob', 'Engineering', 110000),
        (3, 'Carol', 'Engineering', 150000),
        (4, 'Dave', 'Sales', 80000),
        (5, 'Eve', 'Sales', 120000)

Filtering with a local variable
--------------------------------

.. code-block:: sql

    SET LOCAL dept = 'Engineering'

    SELECT id, name FROM employees WHERE department = @dept

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
    * - :json:`1`
      - :json:`"Alice"`
    * - :json:`2`
      - :json:`"Bob"`
    * - :json:`3`
      - :json:`"Carol"`

Reusing a variable across multiple statements
---------------------------------------------

Within a single transaction, one ``SET LOCAL`` applies to all subsequent statements:

.. code-block:: sql

    SET LOCAL threshold = 100000

    SELECT id, name FROM employees WHERE salary > @threshold
    -- Returns Bob, Carol, Eve

    SELECT COUNT(*) AS cnt FROM employees WHERE salary <= @threshold
    -- Returns 2 (Alice, Dave)

Overwriting a variable
-----------------------

.. code-block:: sql

    SET LOCAL x = 1
    SELECT @x   -- returns 1

    SET LOCAL x = 42
    SELECT @x   -- returns 42

See Also
========

* :ref:`create_function` - Defining reusable SQL functions that can reference ``@name`` variables

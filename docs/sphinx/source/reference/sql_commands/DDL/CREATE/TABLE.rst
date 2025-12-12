============
CREATE TABLE
============

Clause in a :ref:`schema template definition <create-schema-template>` to create a table.
Note that a table can either have a primary key defined from one or multiple columns, or it
must be declared as a ``SINGLE ROW ONLY`` table, in which case only one row can be inserted into it.
This latter type of table can be useful for things like database configuration parameters, which the
application wants to inform its interpretation of all data in the database.

Columns can be marked as ``INVISIBLE`` to hide them from ``SELECT *`` queries while still allowing
explicit selection by name. This is useful for backward compatibility when adding new columns,
or for system/computed columns that should not appear in normal queries.

Syntax
======

.. raw:: html
    :file: TABLE.diagram.svg

Parameters
==========

``tableName``
    The name of the ``TABLE``

``columnName``
    The name of a column of the defined ``TABLE``

``columnType``
    The associated :ref:`type <sql-types>` of the column

``INVISIBLE`` / ``VISIBLE``
    Optional visibility modifier for a column:

    - ``INVISIBLE``: Column is excluded from ``SELECT *`` queries but can be explicitly selected
    - ``VISIBLE``: Column is included in ``SELECT *`` queries (default behavior)

``primaryKeyColumnName``
    The name of the column to be part of the primary key of the ``TABLE``

Examples
========

Table with a primary key
------------------------

.. code-block:: sql

    CREATE SCHEMA TEMPLATE TEMP
        CREATE TABLE T (A BIGINT NULL, B DOUBLE NOT NULL, C STRING, PRIMARY KEY(A, B))

    -- On a schema that uses the above schema template
    INSERT INTO T VALUES
        (NULL, 0.0, 'X'),
        (1, 1.0, 'A'),
        (NULL, 2.0, 'B');

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :sql:`NULL`
      - :json:`0.0`
      - :json:`"X"`
    * - :sql:`NULL`
      - :json:`2.0`
      - :json:`"B"`
    * - :json:`1`
      - :json:`1.0`
      - :json:`"A"`


Table limited to a single row
-----------------------------

.. code-block:: sql

    CREATE SCHEMA TEMPLATE TEMP
        CREATE TABLE T (A BIGINT NULL, B DOUBLE NOT NULL, C STRING, SINGLE ROW ONLY)

    -- On a schema that uses the above schema template
    INSERT INTO T VALUES
        (NULL, 0.0, 'X')

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :sql:`NULL`
      - :json:`0.0`
      - :json:`"X"`

Attempting to insert a second row in a :sql:`SINGLE ROW ONLY` table will result in a ``UNIQUE_CONSTRAINT_VIOLATION`` error:

.. code-block:: sql

    INSERT INTO T VALUES
        (1, 2.0, 'X')

    SqlException(23505 - UNIQUE_CONSTRAINT_VIOLATION)

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :sql:`NULL`
      - :json:`0.0`
      - :json:`"X"`

Table with invisible columns
-----------------------------

Invisible columns are excluded from ``SELECT *`` but can be explicitly selected.
This is useful for adding columns without breaking existing queries that use ``SELECT *``.

.. code-block:: sql

    CREATE SCHEMA TEMPLATE TEMP
        CREATE TABLE T (
            id BIGINT,
            name STRING,
            secret STRING INVISIBLE,
            PRIMARY KEY(id)
        )

    -- On a schema that uses the above schema template
    INSERT INTO T VALUES
        (1, 'Alice', 'password123'),
        (2, 'Bob', 'secret456');

    -- SELECT * excludes invisible columns
    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
    * - :json:`1`
      - :json:`"Alice"`
    * - :json:`2`
      - :json:`"Bob"`

.. code-block:: sql

    -- Explicitly selecting invisible columns includes them
    SELECT id, name, secret FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`secret`
    * - :json:`1`
      - :json:`"Alice"`
      - :json:`"password123"`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`"secret456"`

.. code-block:: sql

    -- Invisible columns in subqueries become visible when explicitly selected
    SELECT * FROM (SELECT id, name, secret FROM T) sub;

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`secret`
    * - :json:`1`
      - :json:`"Alice"`
      - :json:`"password123"`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`"secret456"`

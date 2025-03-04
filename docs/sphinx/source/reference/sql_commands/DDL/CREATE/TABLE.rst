============
CREATE TABLE
============

Clause in a :ref:`schema template definition <create-schema-template>` to create a table.
Note that a table can either have a primary key defined from one or multiple columns, or it
must be declared as a ``SINGLE ROW ONLY`` table, in which case only one row can be inserted into it.
This latter type of table can be useful for things like database configuration parameters, which the
application wants to inform its interpretation of all data in the database.

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

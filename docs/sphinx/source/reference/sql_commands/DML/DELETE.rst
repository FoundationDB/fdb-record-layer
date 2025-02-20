======
DELETE
======

SQL command to delete rows from a table.

Syntax
======

.. raw:: html
    :file: DELETE.diagram.svg

Parameters
==========

``tableName``
    The name of the target table

``predicate``
    A predicate which should evaluate to true for the given row to be updated


Examples
========

Delete all rows
---------------

.. code-block:: sql

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`1.0`
    * - :json:`2`
      - :json:`"two"`
      - :json:`2.0`
    * - :json:`3`
      - :json:`"three"`
      - :sql:`NULL`
    * - :json:`4`
      - :json:`"four"`
      - :sql:`NULL`

.. code-block:: sql

    DELETE FROM T;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * -
      -
      -

Update all rows matching a give predicate
-----------------------------------------

.. code-block:: sql

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`1.0`
    * - :json:`2`
      - :json:`"two"`
      - :json:`2.0`
    * - :json:`3`
      - :json:`"three"`
      - :sql:`NULL`
    * - :json:`4`
      - :json:`"four"`
      - :sql:`NULL`

.. code-block:: sql

    DELETE FROM T WHERE C IS NOT NULL;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`3`
      - :json:`"three"`
      - :sql:`NULL`
    * - :json:`4`
      - :json:`"four"`
      - :sql:`NULL`

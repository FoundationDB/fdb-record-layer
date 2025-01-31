======
UPDATE
======

SQL command to update rows in a table.

Syntax
======

.. raw:: html
    :file: UPDATE.diagram.svg

Parameters
==========

``tableName``
    The name of the target table

``columnName``
    The name of one of the target column in the target table

``literal``
    A literal whose type must be compatible with the target column

``identifier``
    A database identifier whose type must be compatible with the target column

``predicate``
    A predicate which should evaluate to true for the given row to be updated

Examples
========

Update a column
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

    UPDATE T SET C = 20;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`20.0`
    * - :json:`2`
      - :json:`"two"`
      - :json:`20.0`
    * - :json:`3`
      - :json:`"three"`
      - :json:`20.0`
    * - :json:`4`
      - :json:`"four"`
      - :json:`20.0`


Update a column for rows that match a certain predicate
-------------------------------------------------------

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

    UPDATE T SET C = NULL WHERE C IS NOT NULL;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :sql:`NULL`
    * - :json:`2`
      - :json:`"two"`
      - :sql:`NULL`
    * - :json:`3`
      - :json:`"three"`
      - :sql:`NULL`
    * - :json:`4`
      - :json:`"four"`
      - :sql:`NULL`


Update multiple columns
-----------------------

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

    UPDATE T SET B = 'zero', C = 0.0 WHERE C IS NULL;

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
      - :json:`"zero"`
      - :json:`0.0`
    * - :json:`4`
      - :json:`"zero"`
      - :json:`0.0`

Update field inside a STRUCT
----------------------------

.. code-block:: sql

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`{ "S1": 10 , "S2": 100 }`
    * - :json:`2`
      - :json:`"two"`
      - :json:`{ "S1": 20 , "S2": 200 }`

.. code-block:: sql

    UPDATE T SET C.S1 = 45 WHERE C.S2 = 200;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`{ "S1": 10 , "S2": 100 }`
    * - :json:`2`
      - :json:`"two"`
      - :json:`{ "S1": 45 , "S2": 200 }`


Update STRUCT field
-----------------------

.. code-block:: sql

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`{ "S1": 10 , "S2": 100 }`
    * - :json:`2`
      - :json:`"two"`
      - :json:`{ "S1": 20 , "S2": 200 }`


.. code-block:: sql

    UPDATE T SET C = (0, 0) WHERE A = 2;

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`{ "S1": 10 , "S2": 100 }`
    * - :json:`2`
      - :json:`"two"`
      - :json:`{ "S1": 0 , "S2": 0 }`


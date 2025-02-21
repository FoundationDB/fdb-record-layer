======
INSERT
======

SQL command to insert rows in a given table. Rows to be inserted can be expressed via a :sql:`VALUES` clause or via a dedicated query.

Syntax
======

.. raw:: html
    :file: INSERT.diagram.svg


Parameters
==========

``tableName``
    The name of the target table

``columnName``
    The name of one of the target column in the target table

``literal``
    A literal whose type must be compatible with the target column

``query``
    A query whose result can be used to insert into the target table

Examples
========

Insert a single row
-------------------

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


.. code-block:: sql

    INSERT INTO T VALUES (3, 'three', 3.0);

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
      - :json:`3.0`


Insert multiple rows
--------------------

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

.. code-block:: sql

    INSERT INTO T VALUES (3, 'three', 3.0), (4, 'four', 4.0);

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
      - :json:`3.0`
    * - :json:`4`
      - :json:`"four"`
      - :json:`4.0`


Insert new rows without specifying all columns
----------------------------------------------

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


.. code-block:: sql

    INSERT INTO T(A, B) VALUES (3, 'three'), (4, 'four');

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


Insert rows in a table with a :sql:`STRUCT` column
--------------------------------------------------

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

    INSERT INTO T VALUES (3, 'three', (30, 300)), (4, 'four', (40, 400));

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
    * - :json:`3`
      - :json:`"three"`
      - :json:`{ "S1": 30 , "S2": 300 }`
    * - :json:`4`
      - :json:`"four"`
      - :json:`{ "S1": 40 , "S2": 400 }`


Insert rows in a table with an ARRAY column
-------------------------------------------

.. code-block:: sql

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`[ 1 ]`
    * - :json:`2`
      - :json:`"two"`
      - :json:`[2, 20]`


.. code-block:: sql

    INSERT INTO T VALUES (3, 'three', [30, 300, 3000]), (4, 'four', [40, 400, 4000, 40000]);

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`A`
      - :sql:`B`
      - :sql:`C`
    * - :json:`1`
      - :json:`"one"`
      - :json:`[ 1 ]`
    * - :json:`2`
      - :json:`"two"`
      - :json:`[2, 20]`
    * - :json:`3`
      - :json:`"three"`
      - :json:`[3, 30, 300]`
    * - :json:`4`
      - :json:`"four"`
      - :json:`[4, 40, 400, 4000]`


Insert from query
-----------------

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

    INSERT INTO T SELECT 3, B, C FROM T WHERE C.S1 = 20

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
    * - :json:`3`
      - :json:`"two"`
      - :json:`{ "S1": 20 , "S2": 200 }`

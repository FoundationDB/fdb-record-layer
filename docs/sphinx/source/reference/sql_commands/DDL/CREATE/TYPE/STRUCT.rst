=====================
CREATE TYPE AS STRUCT
=====================

.. role:: sql(code)
   :language: sql

Clause in a :ref:`schema template definition <create-schema-template>` to create a struct type.

Syntax
======

.. raw:: html
    :file: STRUCT.diagram.svg


Parameters
==========

``structName``
    The name of the :sql:`STRUCT` type

``columnName``
    The name of a column of the defined :sql:`STRUCT`

``columnType``
    The associated :ref:`type <sql-types>` of the column

Example
=======

.. code-block:: sql

    CREATE SCHEMA TEMPLATE TEMP
        CREATE TYPE AS STRUCT coordinates (X DOUBLE, Y DOUBLE)
        CREATE TYPE AS STRUCT route(author STRING, steps coordinates ARRAY)
        CREATE TABLE T(id BIGINT, R route, PRIMARY KEY(id))

    -- On a schema that uses the above schema template
    INSERT INTO T VALUES
        (1, ('Lucy', [(10, 15), (12, 13), (17, 19)])),
        (2, ('Lucy', [(10, 15), (10, 19)])),
        (3, ('Tim', [(10, 19), (10, 15), (7, 9)])),
        (4, ('Tim', [(1, 1), (2, 3), (7, 9)]))

    SELECT * FROM T;

.. list-table::
    :header-rows: 1

    * - :sql:`ID`
      - :sql:`R`
    * - :json:`1`
      - .. code-block:: json

            {
                "AUTHOR" : "Lucy" ,
                "STEPS" : [
                    { "X" : 10.0 , "Y" : 15.0 },
                    { "X" : 12.0 , "Y" : 13.0 },
                    { "X" : 17.0 , "Y" : 19.0 }
                ]
            }
    * - :json:`2`
      - .. code-block:: json

            {
                "AUTHOR" : "Lucy" ,
                "STEPS" : [
                    { "X" : 10.0 , "Y" : 15.0 },
                    { "X" : 10.0 , "Y" : 19.0 }
                ]
            }
    * - :json:`3`
      - .. code-block:: json

            {
                "AUTHOR" : "Tim" ,
                "STEPS" : [
                    { "X" : 10.0 , "Y" : 19.0 },
                    { "X" : 10.0 , "Y" : 15.0 },
                    { "X" : 7.0 , "Y" : 9.0 }
                ]
            }
    * - :json:`4`
      - .. code-block:: json

            {
                "AUTHOR" : "Tim" ,
                "STEPS" : [
                    { "X" : 1.0 , "Y" : 1.0 },
                    { "X" : 2.0 , "Y" : 3.0 },
                    { "X" : 7.0 , "Y" : 9.0 }
                ]
            }

.. code-block:: sql

    SELECT
        ID, R.author, (S.X, S.Y) AS coord
    FROM
        T,
        (SELECT * FROM R.steps) AS S
    WHERE S.X < 10;

.. list-table::
    :header-rows: 1

    * - :sql:`ID`
      - :sql:`AUTHOR`
      - :sql:`COORD`
    * - :json:`3`
      - :json:`"Tim"`
      - :json:`{ "X" : 7.0, "Y": 9.0 }`
    * - :json:`4`
      - :json:`"Tim"`
      - :json:`{ "X" : 1.0, "Y": 1.0 }`
    * - :json:`4`
      - :json:`"Tim"`
      - :json:`{ "X" : 2.0, "Y": 3.0 }`
    * - :json:`4`
      - :json:`"Tim"`
      - :json:`{ "X" : 7.0, "Y": 9.0 }`


####
CAST
####

The CAST operator converts a value from one data type to another. CAST supports explicit type conversions according to SQL standard semantics.

Syntax
######

.. code-block:: sql

    CAST(expression AS target_type)

Parameters
##########

* :sql:`expression`
    The value or expression to be converted.

* :sql:`target_type`
    The target data type. Can be a primitive type (:sql:`INTEGER`, :sql:`BIGINT`, :sql:`FLOAT`, :sql:`DOUBLE`, :sql:`STRING`, :sql:`BOOLEAN`) or an array type (e.g., :sql:`INTEGER ARRAY`, :sql:`STRING ARRAY`).

Supported Conversions
#####################

Numeric Conversions
-------------------

* Integer types: :sql:`INTEGER` ↔ :sql:`BIGINT`
* Floating-poINTEGER types: :sql:`FLOAT` ↔ :sql:`DOUBLE`
* Mixed numeric: :sql:`INTEGER`/:sql:`BIGINT` ↔ :sql:`FLOAT`/:sql:`DOUBLE`
* Narrowing conversions (e.g., :sql:`BIGINT` → :sql:`INTEGER`, :sql:`DOUBLE` → :sql:`FLOAT`) validate range and throw errors on overflow
* Floating-poINTEGER to INTEGER conversions use rounding (Math.round)

String Conversions
------------------

* Any primitive type can be converted to :sql:`STRING`
* :sql:`STRING` can be converted to numeric types (:sql:`INTEGER`, :sql:`BIGINT`, :sql:`FLOAT`, :sql:`DOUBLE`) with validation
* Invalid string-to-numeric conversions throw errors

Boolean Conversions
-------------------

* :sql:`BOOLEAN` → :sql:`INTEGER`: true = 1, false = 0
* :sql:`INTEGER` → :sql:`BOOLEAN`: 0 = false, non-zero = true
* :sql:`STRING` → :sql:`BOOLEAN`: accepts "true"/"1" → true, "false"/"0" → false (case-insensitive)

Array Conversions
-----------------

* Arrays can be cast between compatible element types
* Element type conversion rules follow the same rules as scalar conversions
* Empty arrays can be cast to any array type
* Invalid element conversions cause the entire operation to fail

SQL Standard Compatibility
###########################

This implementation follows SQL standard CAST semantics with the following characteristics:

* Explicit type conversion (unlike implicit promotion)
* Runtime validation with error reporting for invalid conversions
* Range checking for narrowing conversions
* :sql:`NULL` propagation: :sql:`CAST(NULL AS any_type)` returns :sql:`NULL`

**Empty Array Handling**: The system requires explicit :sql:`CAST` for empty array literals. An empty array literal ``[]`` without :sql:`CAST` is invalid and must be written as :sql:`CAST([] AS type ARRAY)` to specify the target element type. This ensures type safety and prevents ambiguity in array operations.

Examples
########

Basic Numeric Conversions
--------------------------

Convert INTEGER to different numeric types:

.. code-block:: sql

    CREATE TABLE numbers(id BIGINT, value INTEGER, PRIMARY KEY(id))
    INSERT INTO numbers VALUES (1, 42)

    SELECT CAST(value AS DOUBLE) AS value_as_double FROM numbers WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`value_as_double`
    * - :json:`42.0`

String to Numeric Conversion
-----------------------------

Parse numeric strings:

.. code-block:: sql

    CREATE TABLE data(id BIGINT, str_value STRING, PRIMARY KEY(id))
    INSERT INTO data VALUES (1, '123')

    SELECT CAST(str_value AS INTEGER) AS parsed_number FROM data WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`parsed_number`
    * - :json:`123`

Numeric to String Conversion
-----------------------------

Convert numbers to strings:

.. code-block:: sql

    CREATE TABLE numbers(id BIGINT, value INTEGER, PRIMARY KEY(id))
    INSERT INTO numbers VALUES (1, 42)

    SELECT CAST(value AS STRING) AS value_as_string FROM numbers WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`value_as_string`
    * - :json:`"42"`

Boolean Conversions
--------------------

Convert between boolean and INTEGER:

.. code-block:: sql

    CREATE TABLE flags(id BIGINT, active BOOLEAN, PRIMARY KEY(id))
    INSERT INTO flags VALUES (1, true), (2, false)

    SELECT CAST(active AS INTEGER) AS active_as_int FROM flags

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`active_as_int`
    * - :json:`1`
    * - :json:`0`

Array Type Conversion
----------------------

Convert arrays between element types:

.. code-block:: sql

    CREATE TABLE arrays(id BIGINT, PRIMARY KEY(id))
    INSERT INTO arrays VALUES (1)

    SELECT CAST([1, 2, 3] AS STRING ARRAY) AS string_array FROM arrays WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`string_array`
    * - :json:`["1", "2", "3"]`

Empty Array Casting
-------------------

Empty arrays must specify target type:

.. code-block:: sql

    CREATE TABLE arrays(id BIGINT, PRIMARY KEY(id))
    INSERT INTO arrays VALUES (1)

    SELECT CAST([] AS INTEGER ARRAY) AS empty_int_array FROM arrays WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`empty_int_array`
    * - :json:`[]`

Nested Conversions
------------------

Combine multiple CAST operations:

.. code-block:: sql

    CREATE TABLE numbers(id BIGINT, value INTEGER, PRIMARY KEY(id))
    INSERT INTO numbers VALUES (1, 42)

    SELECT CAST(CAST(value AS STRING) AS DOUBLE) AS nested_cast FROM numbers WHERE id = 1

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`nested_cast`
    * - :json:`42.0`

Error Handling
##############

:sql:`CAST` operations that fail will raise a :sql:`INVALID_CAST` error (error code ``22F3H``). This includes:

* Invalid string-to-numeric conversions
* Range overflow in narrowing conversions
* Invalid boolean string values
* Incompatible type conversions
* :sql:`NULL` array element types

Invalid Conversions
-------------------

String values that cannot be parsed as numbers result in errors:

.. code-block:: sql

    CREATE TABLE data(id BIGINT, str_value STRING, PRIMARY KEY(id))
    INSERT INTO data VALUES (1, 'invalid')

    SELECT CAST(str_value AS INTEGER) FROM data WHERE id = 1
    -- Error: Cannot cast string 'invalid' to INT
    -- Error Code: 22F3H (INVALID_CAST)

Range Overflow
--------------

Narrowing conversions that exceed target type range result in errors:

.. code-block:: sql

    CREATE TABLE numbers(id BIGINT, PRIMARY KEY(id))
    INSERT INTO numbers VALUES (1)

    SELECT CAST(9223372036854775807 AS INTEGER) FROM numbers WHERE id = 1
    -- Error: Value out of range for INT
    -- Error Code: 22F3H (INVALID_CAST)
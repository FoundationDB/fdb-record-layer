==============
SQL Data Types
==============

.. _sql-types:


Primitive Types
###############

As with most relational databases, the FDB Relational Layer supports the expected types--referred to as "primitive types" in Relational Layer parlance:

* Strings (STRING)
* Scalars (INTEGER/BIGINT)
* Floating Point (FLOAT/DOUBLE)
* Booleans (BOOLEAN)
* byte arrays (BYTES)

In addition, the Relational Layer supports two distinct user-definable types that are core to how it operates: `struct types`_ and `array types`_.


.. _struct_types:

Struct Types
############

You can define a *struct type* (often interchangeably referred to as a *nested type*). A struct is a tuple of columns that allow the same types as a table does, but does _not_ have a primary key. Struct types are "nested" within another owning type, and are stored in the same location as their owning record. For example, a table :sql:`foo` can have the following layout (using the :doc:`DDL <sql_commands/DDL>` syntax):


.. code-block:: sql

    CREATE TYPE AS STRUCT nested_type (d INT64, e STRING);
    CREATE TABLE foo (a STRING, b DOUBLE, c nested_type, PRIMARY KEY(a));

In this example, :sql:`nested_type` is a struct within the table `foo`, and its full contents are materialized alongside the full record for each entry in the `foo` table.

Struct types can have columns which are themselves struct types. Thus, this example is fully allowed:

.. code-block:: sql

    CREATE TYPE AS STRUCT nested_nested_type (f STRING, g STRING);
    CREATE TYPE AS STRUCT nested_type (d INT64, e STRING, f nested_nested_type);
    CREATE TABLE foo (a STRING, b DOUBLE, c nested_type, PRIMARY KEY(a));

In this example, :sql:`nested_type` is a struct within the table :sql:`foo`, and :sql:`nested_nested_type` is a struct within the type :sql:`nested_type`.

The Relational Layer makes no direct limitations on how many structs can be nested within a single type, nor does it limit how deeply nested struct types can be. It is probably worth noting that there are practical limits, like the JVM stack size, that should discourage an adopter from designing types which are nested thousands deep. The general expectation is that nesting depth is probably within the 10s, not 1000s. Note that such "soft" limits may be restructured into hard limits (such as throwing an error if structs are nested more than X level deep) in the future.

.. _array_types:

Array types
###########

The Relational DDL also supports the definition of *array types*. An array is a (finite) collection of records which share the same layout, and which are contained by an owning type. The elements of an array have their own layout, and can be one or more of any accepted column types.

Thus, an array could be of a single primitive type:

.. code-block:: sql

    CREATE TABLE foo (a STRING, b STRING ARRAY);

In this example, `b` is an array with a single :sql:`STRING` column.

Arrays can also be created with struct columns:

.. code-block:: sql

    CREATE TYPE AS STRUCT nested_struct (b STRING, d STRING);
    CREATE TABLE structArray (a STRING, c nested_struct array);

In this example, `c` is an array, and each record within the array is a struct of type :sql:`nested_struct`. You can generally treat an array as a "nested `ResultSet`"--that is to say, you can just pull up a `ResultSet` of an array type, and interrogate it as if it were the output of its own query.

It is possible to nest arrays within structs, and structs within arrays, to an arbitrary depth (limited by the JVM's stack size, currently).

NULL Semantics
##############

For any unset primitive type or struct type fields, queries will return NULL for the column, unless default values are defined.

For array type fields:

* If the whole array is unset, query returns NULL.
* If the array is set to empty, query returns empty list.
* All elements in the array should be set, arrays like :sql:`[1, NULL, 2, NULL]` are not supported.


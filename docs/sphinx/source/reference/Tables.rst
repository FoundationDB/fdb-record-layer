======
Tables
======

A table in the FDB Relational Layer is treated the same way that a table in an alternative. relational database would be.
A table has a name, columns, and metadata which can be investigated.

Tables are *required* to have a primary key defined (except for `Single row tables`_), and they may also have one
or more secondary indexes to improve performance on specific queries. Within a :ref:`schema <schema>`, no two rows in the same
table may share the same primary key value, though tables are allowed to duplicate primary key values if they are defined
in different schemas. Additionally, the :ref:`schema template <schema_template>` option `INTERMINGLE_TABLES` further
specifies that no two rows in the same schema can share the same primary key value, even if the rows are from different
tables. The `INTERMINGLE_TABLES` option allows for polymorphic foreign keys (that is, a row can be referenced solely by
its primary key without regard to type), but it is generally counter-intuitive for those coming from traditional
relational systems, and it is therefore discouraged.

Single row tables
#################

Tables can also be defined to be constrained to one row only:

.. code-block:: sql

    CREATE TABLE single_row_table (a INT64, b INT64, SINGLE ROW ONLY)

Note that there is no explicit primary key, though the "empty value" stands in for the primary key in practice.
Trying to insert more than one row will result in a ``UNIQUE_CONSTRAINT_VIOLATION`` error.


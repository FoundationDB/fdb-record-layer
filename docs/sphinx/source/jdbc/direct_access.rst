======================
Key-Based Data Access
======================

.. important::
   **Transition API**: The Key-Based Data Access API is provided primarily to ease migration from key-value based operations to SQL-based queries. While it remains supported, it is **not the recommended approach for new applications**. For most use cases, SQL queries provide better flexibility, optimization, and maintainability. This API may evolve or be deprecated in future versions as the SQL interface matures.

The Key-Based Data Access API provides programmatic methods for common database operations without writing SQL. These operations work directly with primary keys and support:

- **Scan**: Range queries over primary key prefixes
- **Get**: Single record lookup by complete primary key
- **Insert**: Programmatic record insertion with ``RelationalStruct``
- **Delete**: Key-based deletion (single, batch, and range)

For more complex queries, filtering, joins, or aggregations, use the :doc:`SQL query language <../SQL_Reference>` instead.

Getting Started
===============

The Key-Based Data Access methods are available on ``RelationalStatement``, which extends the standard JDBC ``Statement``. To access these methods, unwrap your statement:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::unwrap-statement[]
         :end-before: // end::unwrap-statement[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::unwrap-statement[]
         :end-before: // end::unwrap-statement[]
         :dedent: 8

Scan Operations
===============

Scans retrieve multiple records that match a primary key prefix. The API supports pagination through continuations for handling large result sets.

Basic Scan
----------

To scan all records in a table, use an empty ``KeySet``:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::scan-basic[]
         :end-before: // end::scan-basic[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::scan-basic[]
         :end-before: // end::scan-basic[]
         :dedent: 8

Scan with Key Prefix
---------------------

Narrow the scan to records matching a primary key prefix. The key columns must form a **contiguous prefix** of the primary key:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::scan-prefix[]
         :end-before: // end::scan-prefix[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::scan-prefix[]
         :end-before: // end::scan-prefix[]
         :dedent: 8

**Key Prefix Rules:**

Given a table with ``PRIMARY KEY(a, b, c)``:

- ✓ Valid: ``{a: 1}``, ``{a: 1, b: 2}``, ``{a: 1, b: 2, c: 3}``
- ✗ Invalid: ``{b: 2}`` (not a prefix), ``{a: 1, c: 3}`` (skips ``b``)

Scan with Continuation
-----------------------

For large result sets, use continuations to paginate through results:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::scan-continuation[]
         :end-before: // end::scan-continuation[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::scan-continuation[]
         :end-before: // end::scan-continuation[]
         :dedent: 8

The continuation points to the **first unread row** after the current position in the ``ResultSet``.

Get Operations
==============

Get operations retrieve a single record by its complete primary key. The result set will contain either 0 or 1 row.

Basic Get
---------

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::get-basic[]
         :end-before: // end::get-basic[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::get-basic[]
         :end-before: // end::get-basic[]
         :dedent: 8

Get with Composite Key
----------------------

For tables with composite primary keys, provide all key columns:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::get-composite[]
         :end-before: // end::get-composite[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::get-composite[]
         :end-before: // end::get-composite[]
         :dedent: 8

**Important**: Unlike scans, Get requires a **complete primary key**. Incomplete keys will cause an error.

Insert Operations
=================

Insert operations add records to a table using ``RelationalStruct`` objects. The struct must contain values for all required columns.

Single Insert
-------------

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::insert-single[]
         :end-before: // end::insert-single[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::insert-single[]
         :end-before: // end::insert-single[]
         :dedent: 8

Batch Insert
------------

Insert multiple records in a single operation for better performance:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::insert-batch[]
         :end-before: // end::insert-batch[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::insert-batch[]
         :end-before: // end::insert-batch[]
         :dedent: 8

Insert with Replace on Duplicate
---------------------------------

Use the ``REPLACE_ON_DUPLICATE_PK`` option to update existing records instead of failing on primary key conflicts:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::insert-replace[]
         :end-before: // end::insert-replace[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::insert-replace[]
         :end-before: // end::insert-replace[]
         :dedent: 8

Delete Operations
=================

Delete operations remove records by their primary keys. Three variants are supported: single delete, batch delete, and range delete.

Single Delete
-------------

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::delete-single[]
         :end-before: // end::delete-single[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::delete-single[]
         :end-before: // end::delete-single[]
         :dedent: 8

Batch Delete
------------

Delete multiple records by providing a collection of keys:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::delete-batch[]
         :end-before: // end::delete-batch[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::delete-batch[]
         :end-before: // end::delete-batch[]
         :dedent: 8

Range Delete
------------

Delete all records matching a primary key prefix using ``executeDeleteRange``:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::delete-range[]
         :end-before: // end::delete-range[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::delete-range[]
         :end-before: // end::delete-range[]
         :dedent: 8

**Warning**: Range deletes can affect many records. Use with caution.

Working with KeySet
===================

``KeySet`` specifies primary key values for direct access operations. It supports fluent builder-style construction:

.. code-block:: java

   KeySet key = new KeySet()
       .setKeyColumn("column1", value1)
       .setKeyColumn("column2", value2);

**Key Points:**

- Column names are case-sensitive and should match your schema
- For scans and range deletes, provide a contiguous key prefix
- For gets and point deletes, provide the complete primary key
- Use ``KeySet.EMPTY`` or ``new KeySet()`` to match all records (full table scan)

Options
=======

The ``Options`` class configures execution behavior for direct access operations. Common options include:

CONTINUATION
------------

Specifies where to resume a scan operation (see continuation example above).

INDEX_HINT
----------

Suggests a specific index for FRL to use:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippets.java
         :language: java
         :start-after: // tag::index-hint[]
         :end-before: // end::index-hint[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/DirectAccessSnippetsServer.java
         :language: java
         :start-after: // tag::index-hint[]
         :end-before: // end::index-hint[]
         :dedent: 8

MAX_ROWS
--------

Limits the number of rows returned in a single scan before prompting for continuation:

.. code-block:: java

   Options options = Options.builder()
       .withOption(Options.Name.MAX_ROWS, 100)
       .build();

REPLACE_ON_DUPLICATE_PK
-----------------------

When inserting, replaces existing records with conflicting primary keys instead of failing (see insert example above).

Building Options
----------------

Use the builder pattern to construct options:

.. code-block:: java

   Options options = Options.builder()
       .withOption(Options.Name.INDEX_HINT, "my_index")
       .withOption(Options.Name.MAX_ROWS, 50)
       .build();

   // Or use Options.NONE for default behavior
   relStmt.executeScan("table", keySet, Options.NONE);

See Also
========

- :doc:`basic` - Basic JDBC usage patterns
- :doc:`advanced` - Working with STRUCTs and ARRAYs
- :doc:`../SQL_Reference` - Complete SQL syntax reference
- :doc:`../reference/Databases_Schemas_SchemaTemplates` - Understanding the data model

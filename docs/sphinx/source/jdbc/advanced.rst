======================
Advanced JDBC Features
======================

.. important::
   The JDBC interface is experimental and not production-ready at this stage. APIs and behaviors are subject to change.

This guide covers FoundationDB Record Layer-specific JDBC features for working with complex data types like STRUCTs and ARRAYs.

.. note::
   **Unified API with Driver-Specific Entry Points**: The Record Layer provides a unified API for creating STRUCT and ARRAY values through common interfaces (``RelationalStruct``, ``RelationalArray``, ``RelationalStructBuilder``, ``RelationalArrayBuilder``). However, you must use the appropriate factory method for your JDBC driver:

   - **Embedded Driver**: ``EmbeddedRelationalStruct.newBuilder()`` and ``EmbeddedRelationalArray.newBuilder()``
   - **Server Driver**: ``JDBCRelationalStruct.newBuilder()`` and ``JDBCRelationalArray.newBuilder()``

   Both factory methods return the same builder interfaces, so once you obtain a builder, all subsequent code is identical regardless of the driver. This design allows the API to abstract over different implementation backends (in-memory objects vs. gRPC serialization) while providing a consistent developer experience.

Working with STRUCT Types
==========================

The Record Layer extends standard JDBC to support STRUCT types, which represent nested record structures similar to protobuf messages. STRUCTs allow you to model hierarchical data within your relational schema.

Reading STRUCT Values
---------------------

To read STRUCT values from a query result, unwrap the ``ResultSet`` to ``RelationalResultSet`` which provides direct access to STRUCTs:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::read-struct[]
   :end-before: // end::read-struct[]
   :dedent: 8

Accessing Nested STRUCT Fields
-------------------------------

STRUCTs can contain other STRUCTs, allowing for deeply nested data structures. Here's how to read a STRUCT with a nested STRUCT:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/ComplexTypesEmbedded.java
   :language: java
   :start-after: // tag::query-struct[]
   :end-before: // end::query-struct[]
   :dedent: 4
   :lines: 1-26

Creating STRUCT Values
----------------------

Use the appropriate builder depending on your JDBC driver. Both builders provide identical APIs. Here's an example inserting a STRUCT into a STRUCT column:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::create-struct-simple[]
   :end-before: // end::create-struct-simple[]
   :dedent: 8

Creating Nested STRUCTs
-----------------------

You can create STRUCTs that contain nested STRUCTs and insert them as a single value. Here's an example that creates a customer STRUCT with a nested address STRUCT:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/ComplexTypesEmbedded.java
   :language: java
   :start-after: // tag::insert-struct[]
   :end-before: // end::insert-struct[]
   :dedent: 4
   :lines: 1-20

Handling NULL STRUCT Fields
----------------------------

You can set NULL values for individual fields within a STRUCT:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::create-struct-null[]
   :end-before: // end::create-struct-null[]
   :dedent: 8

Working with ARRAY Types
=========================

The Record Layer supports ARRAY types containing elements of any SQL type, including primitives, STRUCTs, and even nested ARRAYs.

Reading ARRAY Values
--------------------

Use the ``getArray()`` method to retrieve ARRAY values from a ``ResultSet``:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::read-array-basic[]
   :end-before: // end::read-array-basic[]
   :dedent: 8

Using ResultSet to Iterate Arrays
----------------------------------

You can also retrieve array elements as a ``ResultSet`` for more structured access:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::read-array-resultset[]
   :end-before: // end::read-array-resultset[]
   :dedent: 8

Creating ARRAY Values
---------------------

Use the appropriate builder depending on your JDBC driver. Both builders provide identical APIs:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::create-array-basic[]
   :end-before: // end::create-array-basic[]
   :dedent: 8

Working with Different Array Element Types
-------------------------------------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::create-array-types[]
   :end-before: // end::create-array-types[]
   :dedent: 8

Handling NULL Arrays
--------------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::array-null[]
   :end-before: // end::array-null[]
   :dedent: 8

Complex Nested Types
====================

Arrays of STRUCTs
-----------------

ARRAYs can contain STRUCT elements, allowing for collections of complex records:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
         :language: java
         :start-after: // tag::array-of-structs[]
         :end-before: // end::array-of-structs[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippetsServer.java
         :language: java
         :start-after: // tag::array-of-structs[]
         :end-before: // end::array-of-structs[]
         :dedent: 8

Reading Arrays of STRUCTs
-------------------------

When reading arrays that contain STRUCT elements, unwrap the array's ``ResultSet`` to ``RelationalResultSet``:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/ComplexTypesEmbedded.java
   :language: java
   :start-after: // tag::query-struct[]
   :end-before: // end::query-struct[]
   :dedent: 24
   :lines: 28-47

STRUCTs Containing Arrays
-------------------------

STRUCTs can contain ARRAY fields. You can insert the entire STRUCT including its arrays as a single value:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
         :language: java
         :start-after: // tag::struct-containing-arrays[]
         :end-before: // end::struct-containing-arrays[]
         :dedent: 8

   .. tab-item:: Server Driver
      :sync: server

      .. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippetsServer.java
         :language: java
         :start-after: // tag::struct-containing-arrays[]
         :end-before: // end::struct-containing-arrays[]
         :dedent: 8

Inspecting Array Metadata
==========================

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::array-metadata[]
   :end-before: // end::array-metadata[]
   :dedent: 8

Inspecting STRUCT Metadata
===========================

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/AdvancedSnippets.java
   :language: java
   :start-after: // tag::struct-metadata[]
   :end-before: // end::struct-metadata[]
   :dedent: 8

See Also
========

- :doc:`basic` - Basic JDBC usage patterns
- :doc:`../SQL_Reference` - Complete SQL syntax reference
- :doc:`../reference/Databases_Schemas_SchemaTemplates` - Understanding the data model

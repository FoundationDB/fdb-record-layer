===========
JDBC Guide
===========

.. important::
   The JDBC interface is experimental and not production-ready at this stage. APIs and behaviors are subject to change.

This guide covers using JDBC to interact with the FoundationDB Record Layer's SQL interface.

Driver Setup
============

The Record Layer provides two JDBC drivers depending on your deployment architecture:

- **Embedded Driver**: For applications running in the same JVM as the Record Layer
- **Server Driver**: For client applications connecting to a remote Record Layer server via gRPC

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      The embedded driver class ``com.apple.foundationdb.relational.api.EmbeddedRelationalDriver`` is registered with Java's `Service Loader <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_ and automatically discovered by the JDBC ``DriverManager`` when on the classpath.

      **Maven Dependency:**

      .. code-block:: xml

         <dependency>
             <groupId>org.foundationdb</groupId>
             <artifactId>fdb-relational-core</artifactId>
             <version>VERSION_NUMBER</version>
         </dependency>

      Replace ``VERSION_NUMBER`` with the appropriate version for your project.

   .. tab-item:: Server Driver
      :sync: server

      The server driver class ``com.apple.foundationdb.relational.jdbc.JDBCRelationalDriver`` connects to a remote Record Layer server via gRPC. It is also registered with Java's `Service Loader <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_.

      **Maven Dependency:**

      .. code-block:: xml

         <dependency>
             <groupId>org.foundationdb</groupId>
             <artifactId>fdb-relational-jdbc</artifactId>
             <version>VERSION_NUMBER</version>
         </dependency>

      Replace ``VERSION_NUMBER`` with the appropriate version for your project.

Connection Strings
==================

The connection string format differs between the embedded and server drivers:

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      JDBC URLs use the ``jdbc:embed:`` prefix followed by the database path and query parameters:

      ::

         jdbc:embed:/<database_path>?schema=<schema_name>

      **Examples:**

      Connecting to the system database ``__SYS`` using the ``CATALOG`` schema:

      .. code-block:: java

         String url = "jdbc:embed:/__SYS?schema=CATALOG";
         Connection connection = DriverManager.getConnection(url);

      Connecting to a custom database:

      .. code-block:: java

         String url = "jdbc:embed:/FRL/YCSB?schema=YCSB";
         Connection connection = DriverManager.getConnection(url);

      **Connection Parameters:**

      - **Database path**: The path to the database (e.g., ``/__SYS``, ``/FRL/YCSB``)
      - **schema**: (Required) The schema to use for the connection

   .. tab-item:: Server Driver
      :sync: server

      JDBC URLs use the ``jdbc:relational://`` prefix followed by the server host, database path, and query parameters:

      ::

         jdbc:relational://<host>:<port>/<database_path>?schema=<schema_name>

      **Examples:**

      Connecting to a Record Layer server:

      .. code-block:: java

         String url = "jdbc:relational://localhost:7243/__SYS?schema=CATALOG";
         Connection connection = DriverManager.getConnection(url);

      Connecting to a custom database on a remote server:

      .. code-block:: java

         String url = "jdbc:relational://server.example.com:7243/FRL/YCSB?schema=YCSB";
         Connection connection = DriverManager.getConnection(url);

      **Connection Parameters:**

      - **host**: The hostname or IP address of the Record Layer server
      - **port**: The port number (default: 7243)
      - **Database path**: The path to the database (e.g., ``/__SYS``, ``/FRL/YCSB``)
      - **schema**: (Required) The schema to use for the connection

Basic Usage
===========

Once you have a connection, all JDBC operations work the same way regardless of which driver you're using.

Creating a Connection
---------------------

.. tab-set::

   .. tab-item:: Embedded Driver
      :sync: embedded

      .. code-block:: java

         import java.sql.Connection;
         import java.sql.DriverManager;
         import java.sql.SQLException;

         public class JDBCExample {
             public static void main(String[] args) {
                 String url = "jdbc:embed:/mydb?schema=myschema";

                 try (Connection conn = DriverManager.getConnection(url)) {
                     // Use the connection
                 } catch (SQLException e) {
                     e.printStackTrace();
                 }
             }
         }

   .. tab-item:: Server Driver
      :sync: server

      .. code-block:: java

         import java.sql.Connection;
         import java.sql.DriverManager;
         import java.sql.SQLException;

         public class JDBCExample {
             public static void main(String[] args) {
                 String url = "jdbc:relational://localhost:7243/mydb?schema=myschema";

                 try (Connection conn = DriverManager.getConnection(url)) {
                     // Use the connection
                 } catch (SQLException e) {
                     e.printStackTrace();
                 }
             }
         }

Executing Queries
-----------------

Use ``Statement`` for simple queries:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::simple-query[]
   :end-before: // end::simple-query[]
   :dedent: 8

Executing Updates
-----------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::simple-update[]
   :end-before: // end::simple-update[]
   :dedent: 8

Prepared Statements
===================

Prepared statements provide better security by using parameter binding to prevent SQL injection:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::prepared-insert[]
   :end-before: // end::prepared-insert[]
   :dedent: 8

Querying with Prepared Statements
----------------------------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::prepared-query[]
   :end-before: // end::prepared-query[]
   :dedent: 8

Transaction Management
======================

Auto-Commit Mode
----------------

By default, JDBC connections operate in auto-commit mode where each statement is automatically committed. You can check and modify this behavior:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::autocommit[]
   :end-before: // end::autocommit[]
   :dedent: 8

Manual Transaction Control
---------------------------

For operations that need to execute as a unit, disable auto-commit and manually control transactions:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::transaction[]
   :end-before: // end::transaction[]
   :dedent: 8

Working with ResultSets
=======================

Retrieving Data
---------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::resultset-basic[]
   :end-before: // end::resultset-basic[]
   :dedent: 8

Handling Null Values
--------------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::null-handling[]
   :end-before: // end::null-handling[]
   :dedent: 8

ResultSet Metadata
------------------

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::metadata[]
   :end-before: // end::metadata[]
   :dedent: 8

Error Handling
==============

Proper error handling is essential for robust JDBC applications:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::error-handling[]
   :end-before: // end::error-handling[]
   :dedent: 8

Database Metadata
=================

Retrieve information about the database structure:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/BasicSnippets.java
   :language: java
   :start-after: // tag::database-metadata[]
   :end-before: // end::database-metadata[]
   :dedent: 8

Complete Example
================

Here's a complete example demonstrating common JDBC operations:

.. literalinclude:: ../../../../examples/src/main/java/com/apple/foundationdb/relational/jdbc/examples/ProductManagerEmbedded.java
   :language: java
   :start-after: // tag::complete[]
   :end-before: // end::complete[]
   :dedent: 0

See Also
========

- :doc:`advanced` - Working with STRUCTs and ARRAYs
- :doc:`../SQL_Reference` - Complete SQL syntax reference
- :doc:`../GettingStarted` - Introduction to the Record Layer
- :doc:`../reference/Databases_Schemas_SchemaTemplates` - Understanding the data model

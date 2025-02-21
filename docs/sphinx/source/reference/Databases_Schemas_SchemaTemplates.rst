====================================
Databases, Schemas, Schema Templates
====================================

.. _schema:

Schemas
#######

A *schema* in the Relational Layer is a collection of tables which share a semantic meaning *to the application*. These are
tables that the administrator wants grouped close together on disk, and which share logical relationships with one another.
This is in line with the definition of "schema" that is used by JDBC. Schemas play a large role in how the Relational Layer
is implemented, but from the API perspective they are really just a grouping of related data.

Schemas are represented by a single string label, and schemas are unique *within a database*. That is, for the same database,
no two schemas can share a name. However, different databases may each have a schema with identical names.

.. _database:

Databases
#########

A *database* is a grouping of data which shares access privileges and/or application constraints. Databases are represented
using a URI, which exposes a tree-like structure, so that applications can lay out their databases in a logical way.
Databases are uniquely identified by their path. Only one database *globally* can be represented using that specific path segment.

.. note::

    The mapping of database URI paths to their physical locations in a FoundationDB cluster is currently in flux.
    Adopters should expect changes to allow them more control over how databases are stored.

From a security standpoint, databases are the focal point of security--a user must be authorized to see the contents of a database.

.. warning::

    Authentication and authorization are currently not supported natively by the Relational Layer. For the moment,
    adopters must provide their own solution to validate users prior to connecting to the database. However, aligning
    databases with authorization goals may ready an existing deployment with future capabilities and allow an
    easier migration in the future.

It is assumed that each underlying FoundationDB cluster will maintain millions of databases, and that globally there may be billions
of unique databases within the entirety of a single deployment of the Relational Layer.

.. _schema_template:

Schema Templates
################

One important way that the FDB Relational Layer differs from a "normal" relational database in the DDL language is that
it does *not* support direct schema manipulation. In a typical relational database, one would create a single database,
then within that database define a single schema, and then within that schema define specific tables as a sequence of manual steps.

Instead, in the FDB Relational Layer, we define *schema templates*. A schema template is simply a blueprint for how schemas
are intended to be constructed. To create a schema, you first create a schema template which defines all the tables, indexes,
etc., which are to be defined within any matching schema. Then, you create a schema and explicitly link it by name to schema
template. From then on, the Relational Layer will ensure that the data contained in the schema will be laid out in the
way specifed by the schema template definition.

Functionally, this means that all the table, index, struct, etc., creation statements are within a schema template, rather
than explicit top-level operations. For example, to create a simple `Restaurant` schema, you would do the following:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE restaurant_template
            CREATE TYPE AS STRUCT Location (latitude string, longitude string)
            CREATE TABLE restaurant (rest_no integer, name string, location Location, primary key(rest_no));


    CREATE DATABASE /<uniqueDbPath>
    CREATE SCHEMA /<uniqueDbPath>/<schemaName> WITH TEMPLATE restaurant_template


When you wish to modify a schema's layout (say, to add an index to a specific table), you make the modification instead to
the schema template. The Relational Layer will then automatically migrate *all* schemas which are linked to the specified
template to reflect the modification.

.. warning::

    The storage solution for the schema templates is currently in active development and may change in backwards
    incompatible ways. Adopters should be aware that they may need to migrate any schema templates they write at
    present to a new solution when it becomes available.

It is important to note that it is not possible to modify a schema template without affecting *all* connected schemas. By
similar reasoning, it is not possible to modify an existing schema without modifying its connected schema template.

Though this approach can be cumbersome for some simple use cases that only want to manage a single database, the benefit
of this approach is that it can make it much easier to manage a collection of similar databases. For example, by
sharding data into different databases according to access requirements, users can be granted access only to their
relevant databases, while the database administrator can still easily make changes that are applied to all users.

Bridging a pre-existing Record Layer setup
##########################################

Because of the current limitations around organizing databases and managing schema templates, adopters are advised
that they may be required to use the FDB Record Layer for schema management as this part of the system becomes
more mature. For that reason, they may need to construct a `RecordMetaData` and use a `KeySpacePath` to construct
an `FDBRecordStore` (these Record Layer classes corresponding roughly to a schema template, a database, and a
schema). They can then use the `TransactionBoundDatabase` to use the SQL API to interact with their pre-existing
Record Layer databases. This can allow the adopter to transition to the SQL-based API or make use of new capabilities
only available in the SQL query engine without needing to transition fully to the Relational Layer.
=============
CREATE SCHEMA
=============

Create a schema from a schema template.

Syntax
======

.. raw:: html
    :file: SCHEMA.diagram.svg

Parameters
==========

``pathToSchema``
    The fully-qualified path to the ``SCHEMA`` in a ``DATABASE``

``schemaTemplateName``
    The name of the schema template to be used to create this schema


Example
=======

.. code-block:: sql

    -- Create a schema template
    CREATE SCHEMA TEMPLATE TEMP
        CREATE TABLE T (A BIGINT NULL, B DOUBLE NOT NULL, C STRING, PRIMARY KEY(A, B))

    -- Create database in domain '/FRL/'
    CREATE DATABASE '/FRL/DEMO_DB'

    -- Create a schema in the database from TEMP
    CREATE SCHEMA 'FRL/DEMO_DB/DEMO_SCHEMA' WITH TEMP




===========
DROP SCHEMA
===========

Drop a schema. This drops all the tables and indexes that are part of this schema.

Syntax
======

.. raw:: html
    :file: SCHEMA.diagram.svg

Parameters
==========

``pathToSchema``
    The fully-qualified path of the schema in the database

Example
=======

.. code-block:: sql

    -- drop schema 'DEMO_SCHEMA' in '/FRL/DEMO_DB'
    DROP SCHEMA '/FRL/DEMO_DB/DEMO_SCHEMA'

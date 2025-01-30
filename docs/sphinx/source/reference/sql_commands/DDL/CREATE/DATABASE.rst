===============
CREATE DATABASE
===============

Create a database. A database can contain multiple schemas.

Syntax
======

.. raw:: html
    :file: DATABASE.diagram.svg

Parameters
==========

``pathToDatabase``
    The fully-qualified path of the ``DATABASE``

Example
=======

.. code-block:: sql

    -- Create database in domain '/FRL/'
    CREATE DATABASE '/FRL/DEMO_DB';

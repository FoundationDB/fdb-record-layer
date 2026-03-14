===============
CREATE FUNCTION
===============

.. _create_function:

Creates a user-defined function that encapsulates a SQL query for reuse.

Syntax
======

.. raw:: html
    :file: FUNCTION.diagram.svg

.. raw:: html
    :file: FUNCTION_params.diagram.svg

.. code-block:: sql

    CREATE FUNCTION function_name (
        [IN] parameter_name data_type [DEFAULT default_value], ...
    ) AS query

Parameters
==========

``function_name``
    The name of the function to create. Must be unique within the schema template.

``parameter_name``
    The name of a function parameter. Parameters can be referenced in the function body using their names.

``data_type``
    The SQL data type of the parameter (e.g., ``BIGINT``, ``STRING``, ``INTEGER``).

``default_value``
    Optional default value for the parameter. If provided, the parameter becomes optional when calling the function.

``query``
    The SQL SELECT statement that defines the function body. The query can reference function parameters and use any valid SQL constructs (WHERE clauses, joins, subqueries, etc.).

Returns
=======

Returns the result of executing the function's query with the provided parameter values. The return type and structure depend on the SELECT statement in the function body.

Examples
========

Setup
-----

For these examples, assume we have an ``employees`` table:

.. code-block:: sql

    CREATE TABLE employees(
        id BIGINT,
        name STRING,
        department STRING,
        salary BIGINT,
        PRIMARY KEY(id))

    CREATE INDEX dept_idx AS SELECT department, salary FROM employees ORDER BY department, salary

    INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 100000),
        (2, 'Bob', 'Engineering', 110000),
        (3, 'Carol', 'Engineering', 150000),
        (4, 'Dave', 'Sales', 80000),
        (5, 'Eve', 'Sales', 120000)

Basic Function with Parameters
-------------------------------

Create a function to find employees in a department:

.. code-block:: sql

    CREATE FUNCTION employees_in_dept(IN dept STRING)
    AS SELECT id, name, salary
       FROM employees
       WHERE department = dept

Call the function:

.. code-block:: sql

    SELECT * FROM employees_in_dept('Engineering')

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`salary`
    * - :json:`1`
      - :json:`"Alice"`
      - :json:`100000`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`110000`
    * - :json:`3`
      - :json:`"Carol"`
      - :json:`150000`

Function with Multiple Parameters
----------------------------------

Create a function with multiple filter conditions:

.. code-block:: sql

    CREATE FUNCTION employees_by_dept_and_salary(
        IN dept STRING,
        IN min_salary BIGINT
    ) AS SELECT id, name, salary
         FROM employees
         WHERE department = dept AND salary >= min_salary

Call with positional parameters:

.. code-block:: sql

    SELECT * FROM employees_by_dept_and_salary('Engineering', 110000)

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`salary`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`110000`
    * - :json:`3`
      - :json:`"Carol"`
      - :json:`150000`

Function with DEFAULT Parameters
---------------------------------

Create a function with optional parameters:

.. code-block:: sql

    CREATE FUNCTION search_employees(
        IN dept STRING DEFAULT 'Engineering',
        IN min_salary BIGINT DEFAULT 0
    ) AS SELECT id, name, department, salary
         FROM employees
         WHERE department = dept AND salary >= min_salary

Call with all defaults:

.. code-block:: sql

    SELECT * FROM search_employees()

Returns all employees in 'Engineering' with salary >= 0.

Call with named parameters (in any order):

.. code-block:: sql

    SELECT * FROM search_employees(min_salary => 100000, dept => 'Sales')

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`department`
      - :sql:`salary`
    * - :json:`5`
      - :json:`"Eve"`
      - :json:`"Sales"`
      - :json:`120000`

Using Functions in JOINs
-------------------------

Functions can be used like tables in FROM clauses and joins:

.. code-block:: sql

    SELECT A.name AS emp1, B.name AS emp2
    FROM employees_in_dept('Engineering') A,
         employees_in_dept('Engineering') B
    WHERE A.id < B.id

This performs a self-join on Engineering employees.

Using Functions in Subqueries
------------------------------

Functions can be used in correlated subqueries:

.. code-block:: sql

    CREATE FUNCTION same_dept_employees(IN emp_dept STRING)
    AS SELECT id, name FROM employees WHERE department = emp_dept

    SELECT name, department
    FROM employees e
    WHERE EXISTS (
        SELECT * FROM same_dept_employees(e.department)
        WHERE id > e.id
    )

This finds employees who have colleagues with higher IDs in the same department.

Nested Function Calls
----------------------

Functions can call other functions:

.. code-block:: sql

    CREATE FUNCTION high_earners(IN dept STRING)
    AS SELECT * FROM employees_in_dept(dept) WHERE salary > 100000

    SELECT * FROM high_earners('Engineering')

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
      - :sql:`salary`
    * - :json:`2`
      - :json:`"Bob"`
      - :json:`110000`
    * - :json:`3`
      - :json:`"Carol"`
      - :json:`150000`

Important Notes
===============

Named vs Positional Parameters
-------------------------------

Function parameters can be passed in two ways:

**Positional parameters** (in the order defined):

.. code-block:: sql

    SELECT * FROM my_function(value1, value2)

**Named parameters** (any order, using ``=>`` syntax):

.. code-block:: sql

    SELECT * FROM my_function(param2 => value2, param1 => value1)

Named parameters are especially useful when:
- Working with functions that have many parameters
- Using functions with DEFAULT parameters
- Improving code readability

**Calling without parentheses**: If all function parameters have DEFAULT values, you can call the function without parentheses:

.. code-block:: sql

    -- Function with all parameters having defaults
    CREATE FUNCTION get_all_engineers(
        IN dept STRING DEFAULT 'Engineering',
        IN min_salary BIGINT DEFAULT 0
    ) AS SELECT * FROM employees WHERE department = dept AND salary >= min_salary

    -- Can be called without parentheses
    SELECT * FROM get_all_engineers

This is equivalent to calling ``get_all_engineers()`` with all default values.

Function Scope
--------------

Functions are defined at the schema template level and are available in all schemas created from that template. Function names must be unique within a schema template.

Parameter Expressions
---------------------

When calling functions, parameters can be:
- Literal values: ``my_function(100, 'text')``
- Column references: ``my_function(employee.id, employee.name)``
- Expressions: ``my_function(salary * 1.1, CONCAT(first_name, ' ', last_name))``

Query Restrictions
------------------

The function body must be a SELECT statement. It can include:
- WHERE clauses
- Joins (using comma-separated FROM syntax)
- Subqueries
- Aggregate functions (with appropriate GROUP BY)
- ORDER BY clauses (with appropriate indexes)

Functions cannot contain:
- Data modification statements (INSERT, UPDATE, DELETE)
- DDL statements (CREATE, DROP, ALTER)
- Multiple statements

See Also
========

* :ref:`Subqueries <subqueries>` - Using functions in subqueries
* :ref:`Joins <joins>` - Using functions in join operations

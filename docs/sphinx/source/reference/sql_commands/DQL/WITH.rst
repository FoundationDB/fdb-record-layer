#####
WITH
#####

A common table expression (CTE) defines a named query that can referenced multiple times within the scope of a SQL statement.
CTEs are visible within the query block in which they are defined, and they can be nested.

Both non-recursive and recursive CTEs are supported.

Syntax
######

.. raw:: html
    :file: WITH.diagram.svg

Parameters
##########

* :sql:`RECURSIVE`
    Optional keyword to enable recursive functionality. When specified, the CTE can reference itself in its query definition.

* :sql:`TRAVERSAL ORDER`
    Optional clause for recursive CTEs that specifies the traversal strategy. This clause appears before the final SELECT statement:

    - :sql:`pre_order` - Depth-first search (DFS) pre-order traversal: visits parent nodes before their children
    - :sql:`post_order` - Depth-first search (DFS) post-order traversal: visits child nodes before their parents
    - :sql:`level_order` - Breadth-first (level-order) traversal: visits all nodes at each level before proceeding to the next level
    - If not specified, the optimizer chooses the best traversal strategy (default)

* :sql:`cteName`
    The name of the common table expression.

* :sql:`columnAlias`
    The alias of the corresponding column of the CTE query.

* :sql:`cteQuery`
    The common table expression query. For recursive CTEs, this must be structured as an anchor query UNION ALL recursive query.

* :sql:`query`
    The query that can optionally reference the CTE query via its given `cteName`.

Important Notes
###############

**For all CTEs:**

* CTEs are visible within the query block in which they are defined
* Multiple CTEs can be defined in a single WITH clause, separated by commas
* CTEs can be nested within other CTEs

**For Recursive CTEs:**

* Must use :sql:`UNION ALL` (not :sql:`UNION`) between the anchor and recursive queries
* The recursive query must reference the CTE name to establish the recursive relationship
* **Warning**: The system does not automatically prevent infinite recursion. Ensure your recursive queries have proper
  termination conditions to avoid infinite loops that may cause the system to crash
* Traversal strategy affects the order of results but not the final set of results
* Column names and types must be consistent between anchor and recursive queries

Examples
########

Simple CTE
----------

Suppose we have the following table representing an employee hierarchy:

.. code-block:: sql

    CREATE TABLE employees(id BIGINT, name STRING, manager_id BIGINT, dept STRING, PRIMARY KEY(id))
    INSERT INTO employees VALUES
        (1, 'Alice', NULL, 'Executive'),
        (2, 'Bob', 1, 'Engineering'),
        (3, 'Carol', 1, 'Marketing'),
        (4, 'David', 2, 'Engineering'),
        (5, 'Eve', 2, 'Engineering'),
        (6, 'Frank', 3, 'Marketing')

Here is a simple CTE query to find all engineering employees:

.. code-block:: sql

    WITH engineering_team(name) AS (
        SELECT name FROM employees WHERE dept = 'Engineering'
    )
    SELECT * FROM engineering_team

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Bob"`
    * - :json:`"David"`
    * - :json:`"Eve"`

Recursive CTEs
--------------

Recursive CTEs allow traversing hierarchical relationships. Consider the employee hierarchy as a tree structure:

.. code-block:: text

    Alice (CEO, id: 1)
    ├── Bob (Engineering Manager, id: 2)
    │   ├── David (Engineer, id: 4)
    │   └── Eve (Engineer, id: 5)
    └── Carol (Marketing Manager, id: 3)
        └── Frank (Marketing Associate, id: 6)

Finding All Subordinates
^^^^^^^^^^^^^^^^^^^^^^^^

Find all employees who report to Alice (directly or indirectly):

.. code-block:: sql

    WITH RECURSIVE subordinates AS (
        -- Anchor: Start with Alice
        SELECT id, name, manager_id FROM employees WHERE id = 1
        UNION ALL
        -- Recursive: Find direct reports of current level
        SELECT e.id, e.name, e.manager_id
        FROM subordinates AS s, employees AS e
        WHERE s.id = e.manager_id
    )
    SELECT name FROM subordinates WHERE id != 1

Result with optimizer-chosen traversal:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Bob"`
    * - :json:`"David"`
    * - :json:`"Eve"`
    * - :json:`"Carol"`
    * - :json:`"Frank"`

Finding Management Chain
^^^^^^^^^^^^^^^^^^^^^^^^

Find the management chain from an employee up to the CEO:

.. code-block:: sql

    WITH RECURSIVE management_chain AS (
        -- Anchor: Start with a specific employee
        SELECT id, name, manager_id FROM employees WHERE name = 'Eve'
        UNION ALL
        -- Recursive: Find manager of current employee
        SELECT e.id, e.name, e.manager_id
        FROM management_chain AS mc, employees AS e
        WHERE mc.manager_id = e.id
    )
    SELECT name FROM management_chain

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Eve"`
    * - :json:`"Bob"`
    * - :json:`"Alice"`

Level-Order Traversal
^^^^^^^^^^^^^^^^^^^^^

Using level traversal to process the hierarchy breadth-first:

.. code-block:: sql

    WITH RECURSIVE org_levels AS (
        SELECT id, name, manager_id, 0 AS level FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.manager_id, ol.level + 1
        FROM org_levels AS ol, employees AS e
        WHERE ol.id = e.manager_id
    )
    TRAVERSAL ORDER level_order
    SELECT name, level FROM org_levels ORDER BY level, name

This processes all employees at level 0 (CEO), then level 1 (direct reports), then level 2, etc.

Result with level traversal (breadth-first):

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`level`
    * - :json:`"Alice"`
      - :json:`0`
    * - :json:`"Bob"`
      - :json:`1`
    * - :json:`"Carol"`
      - :json:`1`
    * - :json:`"David"`
      - :json:`2`
    * - :json:`"Eve"`
      - :json:`2`
    * - :json:`"Frank"`
      - :json:`2`

Preorder Traversal
^^^^^^^^^^^^^^^^^^

Using explicit preorder traversal to process the hierarchy depth-first:

.. code-block:: sql

    WITH RECURSIVE subordinates AS (
        SELECT id, name, manager_id FROM employees WHERE id = 1
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM subordinates AS s, employees AS e
        WHERE s.id = e.manager_id
    )
    TRAVERSAL ORDER pre_order
    SELECT name FROM subordinates WHERE id != 1

Result with preorder traversal (depth-first):

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Bob"`
    * - :json:`"David"`
    * - :json:`"Eve"`
    * - :json:`"Carol"`
    * - :json:`"Frank"`

Notice how preorder explores Bob's entire subtree (David, Eve) before moving to Carol's subtree (Frank), while level traversal processes all level 1 employees (Bob, Carol) before any level 2 employees.

Postorder Traversal
^^^^^^^^^^^^^^^^^^^

Using postorder traversal to process the hierarchy depth-first, visiting children before their parents. This is particularly useful for bottom-up computations like calculating team sizes, aggregating metrics, or determining dependencies where leaf nodes must be processed before their ancestors.

.. code-block:: sql

    WITH RECURSIVE subordinates AS (
        SELECT id, name, manager_id FROM employees WHERE id = 1
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM subordinates AS s, employees AS e
        WHERE s.id = e.manager_id
    )
    TRAVERSAL ORDER post_order
    SELECT name FROM subordinates WHERE id != 1

Result with postorder traversal (children before parents):

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"David"`
    * - :json:`"Eve"`
    * - :json:`"Bob"`
    * - :json:`"Frank"`
    * - :json:`"Carol"`

Notice how postorder processes all leaf employees (David, Eve, Frank) before their managers (Bob, Carol). This is in contrast to preorder which processes managers before their direct reports. Postorder is ideal for operations that require child data to be available before processing parents, such as calculating subtree sizes or propagating values up a hierarchy.

The key difference between the traversal orders:

- **Preorder** (top-down): Alice → Bob → David → Eve → Carol → Frank
- **Postorder** (bottom-up): David → Eve → Bob → Frank → Carol → Alice
- **Level-order** (breadth-first): Alice → (Bob, Carol) → (David, Eve, Frank)

Nested Recursive CTEs
^^^^^^^^^^^^^^^^^^^^^

Complex hierarchical operations using nested recursive CTEs to analyze relationships across multiple organizational divisions:

Consider a more complex scenario with multiple organizational divisions:

.. code-block:: sql

    CREATE TABLE employees(id BIGINT, name STRING, manager_id BIGINT, dept STRING, PRIMARY KEY(id))
    INSERT INTO employees VALUES
        -- Tech Division (Alice is CEO)
        (1, 'Alice', NULL, 'Executive'),
        (2, 'Bob', 1, 'Engineering'),
        (3, 'Carol', 1, 'Engineering'),
        (4, 'David', 2, 'Engineering'),
        (5, 'Eve', 2, 'Engineering'),
        (6, 'Frank', 3, 'Engineering'),
        (7, 'Grace', 3, 'Engineering'),
        -- Sales Division (Helen is independent CEO)
        (10, 'Helen', NULL, 'Sales'),
        (11, 'Ivan', 10, 'Sales'),
        (12, 'Julia', 10, 'Sales'),
        (13, 'Kevin', 11, 'Sales'),
        (14, 'Lisa', 11, 'Sales'),
        -- Marketing Division (Mike is independent CEO)
        (20, 'Mike', NULL, 'Marketing'),
        (21, 'Nina', 20, 'Marketing'),
        (22, 'Oscar', 21, 'Marketing')

This creates a forest of three separate organizational hierarchies:

.. code-block:: text

    Tech Division:          Sales Division:         Marketing Division:
    Alice (CEO)             Helen (CEO)             Mike (CEO)
    ├── Bob                 ├── Ivan                └── Nina
    │   ├── David           │   ├── Kevin               └── Oscar
    │   └── Eve             │   └── Lisa
    └── Carol               └── Julia
        ├── Frank
        └── Grace

Find all employees who report to any of Bob's managers (colleagues in Bob's management chain):

.. code-block:: sql

    WITH RECURSIVE colleagues_of_bob AS (
        -- First: Find all managers in Bob's chain of command (ancestors)
        WITH RECURSIVE bob_managers AS (
            SELECT id, name, manager_id FROM employees WHERE name = 'Bob'
            UNION ALL
            SELECT e.id, e.name, e.manager_id
            FROM bob_managers AS bm, employees AS e
            WHERE bm.manager_id = e.id
        )
        SELECT id, name, manager_id FROM bob_managers
        UNION ALL
        -- Second: Find all employees who report to any of Bob's managers
        SELECT e.id, e.name, e.manager_id
        FROM colleagues_of_bob AS c, employees AS e
        WHERE c.id = e.manager_id AND NOT EXISTS (SELECT name FROM c WHERE name = e.name)
    )
    SELECT name FROM colleagues_of_bob WHERE name != 'Bob'

This query demonstrates several advanced concepts. The nested recursive CTEs allow the inner ``bob_managers`` CTE to
find Bob's management chain (ancestors), while the outer ``colleagues_of_bob`` CTE uses those results to find all
related employees. The ``NOT EXISTS (SELECT name FROM c WHERE name = e.name)`` predicate prevents adding employees
who are already in the result set - without this, employees could be added multiple times through different management
paths. The query uses two-phase processing: first finding Bob's management chain (Bob → Alice), then finding all
employees who report to anyone in that chain.

Result:

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Alice"`
    * - :json:`"Carol"`
    * - :json:`"David"`
    * - :json:`"Eve"`
    * - :json:`"Frank"`
    * - :json:`"Grace"`

The forest structure ensures we only get employees from Bob's organizational division (Tech), not from Sales or
Marketing divisions. The existential predicate is crucial for preventing infinite loops and duplicate results in
complex recursive hierarchies.

Performance Characteristics
###########################

The choice between LEVEL, PREORDER, and POSTORDER traversal strategies can impact query performance, but the behavior is heavily affected by the specific hierarchy profile and usage patterns. Users should carefully consider these factors when selecting a traversal strategy:

**Key Considerations:**

- **Hierarchy Profile**: The structure of your data (wide vs. deep hierarchies) significantly influences which strategy performs better
- **Continuation Usage**: If you plan to use query continuations for pagination, the continuation size differences are substantial
- **Dataset Size**: Performance characteristics may vary significantly based on the total number of nodes in your hierarchy

**Continuation Size Behavior:**

One clear difference between the strategies is continuation size:

- **PREORDER & POSTORDER**: Generate very small continuation sizes (typically <50 bytes)
- **LEVEL**: Produces much larger continuation sizes (often several KB to hundreds of KB)

This difference can be critical if you're implementing pagination or need to serialize query state.

**Execution Time Observations:**

Initial benchmarking suggests performance differences exist between the strategies, but results vary significantly based on hierarchy characteristics. Some observations from testing:

.. list-table::
    :header-rows: 1

    * - Dataset Size
      - Tree Structure
      - LEVEL Time
      - PREORDER Time
      - Notes
    * - 1,000 nodes
      - 10 levels, wide branching
      - 405ms
      - 898ms
      - LEVEL faster for wide trees
    * - 10,000 nodes
      - 10 levels, wide branching
      - 3,097ms
      - 7,620ms
      - Pattern continues
    * - Deep hierarchies
      - 100+ levels, narrow branching
      - Variable
      - Variable
      - Results depend on specific structure

**Recommendations:**

- **Test with your data**: Performance characteristics are highly dependent on your specific hierarchy structure
- **Consider continuation needs**: If you need small continuation sizes for pagination, PREORDER has a significant advantage
- **Monitor memory usage**: Different strategies may have varying memory requirements based on your dataset
- **Benchmark your use case**: Execution time performance is still under investigation and may vary significantly based on your specific queries and data patterns

The performance behavior of recursive CTEs is an active area of development and optimization. Always test with representative data before making final decisions about traversal strategies.

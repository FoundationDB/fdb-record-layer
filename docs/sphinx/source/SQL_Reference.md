# SQL reference

```{note}
The SQL interface of the FoundationDB Record Layer is under active development and, along with this reference, is expected to change frequently as the SQL engine develops.
```

The FoundationDB Record Layer includes a **Relational Layer**, which provides a SQL interface for interacting with record stores. It includes a full query engine with sophisticated query planning. We generally recommend using this interface over the {doc}`direct-access API <jdbc/direct_access>` whenever possible, as the query planner is more capable and automatically handles decisions such as selecting appropriate indexes.

The query language closely follows standard SQL, although the Relational Layer makes some deliberate changes to accommodate the nature of its data model. Most notably, database schemas are instantiated based on {ref}`schema templates <schema_template>` and, in practice, make heavy use of {ref}`indexes <index_definition>` as well as {ref}`arrays <array_types>` and user-defined {ref}`struct <struct_types>` data types.

```{toctree}
:maxdepth: 2

reference/Concepts
reference/sql_commands/DDL
reference/sql_commands/DML
reference/sql_commands/DQL
reference/query_options
reference/sql_types
reference/Expressions
reference/Functions
reference/Aggregates
```

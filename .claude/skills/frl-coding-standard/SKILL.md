---
name: frl-coding-standard
description: Coding standards for all Java code in fdb-record-layer. Apply when writing or reviewing any Java code.
  Some example usages:
  "Review this class for coding standards"
  "Write a new implementation of RecordCursor"
---

All Java, Gradle, and property files MUST end with a newline character.

Target Java 17 language level, compiled with JDK 21.

## Exceptions

The two layers use different exception systems. **Know which layer you are in.**

### fdb-record-layer-* (record layer)

Exceptions extend `RecordCoreException`, which extends `LoggableException` (unchecked).

- The exception message MUST be a **static string** — no string interpolation or variables.
- Use `.addLogInfo(LogMessageKeys.KEY, value)` to attach context. Include as much as needed
  to diagnose the error: subspace, primary key, record type, index name, etc.

```java
// Wrong
throw new RecordCoreException("Found corrupt record: " + record.getPrimaryKey());

// Correct
throw new RecordCoreException("Found corrupt record")
    .addLogInfo(LogMessageKeys.PRIMARY_KEY, record.getPrimaryKey())
    .addLogInfo(LogMessageKeys.RECORD_TYPE, record.getRecordType());
```

### fdb-relational-* (relational / SQL layer)

Exceptions are `RelationalException` (checked, extends `Exception`). Always pair with an
`ErrorCode`, which maps to a SQLSTATE-standard 5-digit code.

- Message must still be a **static string**.
- Attach context with `.addContext("key", value)` — this travels with the exception for
  logging and is preserved when converting to `SQLException`.

```java
// Minimal form
throw new RelationalException("Transaction rolled back", ErrorCode.SERIALIZATION_FAILURE);

// With context
throw new RelationalException("Unsupported type in column", ErrorCode.CANNOT_CONVERT_TYPE)
    .addContext("columnName", colName)
    .addContext("typeName", typeName);
```

At JDBC interface boundaries (where the signature throws `SQLException`), convert via
`.toSqlException()`, which produces a `ContextualSQLException` carrying the `ErrorCode`
as the SQLState and the context map for logging:

```java
throw new RelationalException("Element is not of STRUCT type", ErrorCode.CANNOT_CONVERT_TYPE)
    .toSqlException();
```

Specialized subclasses (`InvalidColumnReferenceException`, `InvalidTypeException`) exist for
common error cases — prefer them when they fit. `InternalErrorException` is deprecated; use
`new RelationalException("...", ErrorCode.INTERNAL_ERROR)` directly.

`UncheckedRelationalException` wraps a `RelationalException` as a `RuntimeException` for
contexts where checked exceptions cannot be thrown (e.g., inside lambdas). Unwrap with
`.unwrap()` to recover the original `RelationalException`.

## Logging

Same principle as exceptions — structured, static messages:

- Always use `KeyValueLogMessage.of("static message", key, value, ...)`.
- The message text must be a static string. Variables go in key/value pairs.

```java
// Wrong
LOGGER.info("Unable to open file: " + filename);

// Correct
LOGGER.info(KeyValueLogMessage.of("Unable to open file", LogMessageKeys.FILENAME, filename));
```

## Futures (async code)

The `fdb-record-layer-*` and lower-level parts of `fdb-relational-core` are fully asynchronous
via `CompletableFuture`. The JDBC-facing relational API presents a synchronous surface, but
async code is common in the implementation beneath it.

- **Never** call `join()` or `get()` in production code. Use `context.asyncToSync()` or
  `database.asyncToSync()` instead — they enforce timeouts, record wait timers, and handle
  error wrapping.
- **Never** call any blocking method from inside a `CompletableFuture` completion lambda. The
  thread pool is finite; blocking inside a future can deadlock the entire system.
- **Avoid** `thenApplyAsync()` / `thenComposeAsync()` unless the lambda does significant CPU
  work. These enqueue a new task on the executor for no benefit in the common case. When you
  do need them, always pass an explicit `Executor`.
- **Always pass an `Executor`** to any `AsyncUtil` or `MoreAsyncUtil` method that accepts one
  (e.g. `AsyncUtil.whileTrue()`). Never use the overload without an executor.

## Checkstyle-banned imports

The authoritative list is in `gradle/check.gradle` (`BANNED_IMPORTS`).

## Inline comments

Describe **intent**, not mechanics. Explain the *why* — a hidden constraint, a non-obvious
invariant, a workaround for a specific FDB behavior. If removing the comment would not confuse
a future reader, don't write it.

## Javadoc

Add Javadoc for public APIs. It is not necessary for private methods unless the logic is
genuinely non-obvious.

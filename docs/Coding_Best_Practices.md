# FoundationDB Record Layer Coding Best Practices

This is a living document aimed towards documenting and clarifying our
coding best practives.

## Exceptions 

### Exception structure

Almost all `Exception`s thrown in Record Layer extend `RecordCoreException`, and
`RecordCoreException`, itself, extends `LoggableException`. The rules around 
`LoggableExceptions` are:

* The text of the exception should be static (no variables in it)
* Logging properties should be added to give context to the error (where variables would have been used)
* The logging properties should provide as much detail as possible to diagnose the source of the error

For example:

```
throw new RecordCoreException("Found corrupt record: " + record.getPrimarykey());
```

should be written as:

```
throw new RecordCoreException("Found corrupt record")
    .withLogInfo(LogMessageKeys.PRIMARY_KEY, record.getPrimaryKey());
```

However, at scale, where you may have a system managing dozens or thousands of
record stores, just know the primary key of a record probably isn't sufficient
to track down the problem, so care should be taken to include as much context as
possible around the issue, such as:

```
throw new RecordCoreException("Found corrupt record").withLogInfo(
    LogMessageKeys.SUBSPACE, store.getRecordSubspace(),
    LogMessageKeys.PRIMARY_KEY, record.getPrimaryKey(),
    LogMessageKeys.RECORD_TYPE, record.getRecordType());
```

By structuring our exceptions this way, we get a number of nice benefits:

* We can easily search our logs for all occurrances of a given exception, 
  such as finding how often "Found corrupt record" occurs
* For a given exception, we can filter or group on the logging properties,
  such as counting the number of corrupt records by record type
* At a broader scale, without having variables polluting the text of the
  exception, we can trivially do things like determine which exception
  happens most frequently in the system, by performing a count grouped by
  the exception title. 

## Logging

Logging in record layer follows a similar paradigm to that described in 
**Exception structure**, above.  Namely:

* The value logged should always be a `KeyValueLogMessage`
* The text of `KeyValueLogMessage` must always be a static string, with no variables
* Logging properties should be added to give context to the error (where 
  variables would have been used)
* The logging properties should provide as much detail as possible to diagnose the 
  source of the error

For example, instead of logging:

```
LOGGER.info("Unable to open file: " + filename);
```

you should instead log:

```
LOGGER.info(KeyValueLogMessage.of("Unable to open file",
    LogMessageKeys.FILENAME, filename));
```

## Futures

### `join()` or `get()` in production code

Production code (i.e., code that isn't test code), should never call `CompletableFuture.join()` 
or `CompletableFuture.get()`, but instead it should call `FDBRecordContext.asyncToSync()` or
`FDBDatabase.asyncToSync()`.  The `asyncToSync()` method provides a number of important
benefits:

* It establishes a client-configurable time limit on how long we are 
  willing to wait for the operation to complete
* It automatically defines a wait timer that will be included in the `FDBStoreTimer`
  to track the time spent waiting on the event
* It takes care of error handling and wrapping to promote thrown exceptions to the 
  appropriate `RecordCoreException`
* It has the ability to detect if `asyncToSync()` is being called from within a 
  completing future (see **Blocking in asyncronous code**, below)

### Blocking in asynchronous code

The methods `join()`, `get()`, `asyncToSync()` or any method that is known to block 
(such as syncronous I/O methods) should never be called from within a completable 
future.

The thread pool `Executor` has a limited number of threads that are used to invoke 
the lambda in the `CompletableFuture` when it completes, so if that future
makes a blocking call, it has permenantly consumed the `Executor` thread until 
the completion of that operation. In the case where the blocking call is waiting
for another future to complete (e.g., `asyncToSync()`, `join()`, or `get()`), you
can end up in a situation where the thread pool is entirely consumed with futures
blocking for completions, and no threads are available to complete those requests,
and the entire system deadlocks. 

In the Record Layer unit tests (any test extending `FDBTestBase`), the `asyncToSync()`
call will automatically attempt to detect situations in which it finds itself being
invoked from within a `CompletableFuture` completion and throw an exception.

### `.thenApplyAsync` and `.thenComposeAsync`

In most situations, you do not want to use these methods, but should use `thenApply()` 
or `thenCompose()`. The `CompletableFuture.*Async()` methods create and enqueue a new future 
that will execute the code in the lambda, rather than executing the code in the lambda 
immediately upon completion of this future completes. For example:

```
fetchSomeNumber().thenApplyAsync(number -> number * 10);
```

will create an entirely new future that is enqueued on the `Executor` thread, simply to 
do the multiplication of the number. Whereas:

```
fetchSomeNumber().thenApply(number -> number * 10);
```

Will do the calculation immediately, as soon as `number` becomes available.  So the 
former form is FAR more expensive, for very little gain.  The main use of the `*Async()` 
methods are cases where the lambda needs to do something relatively expensive (e.g., factor 
the number) and you don't want to block the current future chain while it happens. 

Further, in the cases where you *do* need to use the `*Async()` methods, be sure you 
pass it an `Executor`, like:

```
fetchSomeNumber().thenApplyAsync(number -> number * 10, context.getExecutor());
```

Otherwise it is executed in the global thread pool, rather than the Record Layer 
thread pool.

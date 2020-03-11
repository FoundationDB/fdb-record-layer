package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A task that can be supplied to an {@link AsyncLoadingCache}. This allows for an asynchronous task to be
 * started and then cancelled later in the future.
 *
 * <p>
 * Note that this API is experimental. It seems likely that as there is a broader, underlying problem on how
 * to manage futures in a way that actually cancels the underlying work, it is possible that this API changes
 * or is pushed down in the future.
 * </p>
 *
 * @param <T> the type
 */
@API(API.Status.EXPERIMENTAL)
public interface AsyncLoadingTask<T> extends AutoCloseable {
    /**
     * Asynchronously load a value. Implementors can assume that this method will only be called
     * once.
     */
    CompletableFuture<T> load();

    /**
     * Close any outstanding work associated with this task if possible. Note that unlike the close method
     * in {@link AutoCloseable}s, this is not allowed to throw a checked exception.
     */
    @Override
    void close();

    /**
     * Create an {@link AsyncLoadingTask} from a {@link Supplier} of a {@link CompletableFuture}. When
     * {@link #load()} is called, the {@link Supplier} will be called to provide the given future. When
     * {@link #close()} is called, the future, if created, will be cancelled. Note, however, that cancelling
     * a future is not necessarily guaranteed to cancel any of the underlying work.
     *
     * @param futureSupplier a supplier of a {@link CompletableFuture}
     * @param <T> the type returned by this future
     * @return a task that wrapping the given supplier
     */
    static <T> AsyncLoadingTask<T> fromFutureSupplier(@Nonnull Supplier<CompletableFuture<T>> futureSupplier) {
        return new AsyncLoadingTask<T>() {
            @Nullable
            private CompletableFuture<T> future;

            @Override
            public CompletableFuture<T> load() {
                if (future == null) {
                    future = futureSupplier.get();
                }
                return future;
            }

            @Override
            public void close() {
                if (future != null) {
                    future.cancel(true);
                }
            }
        };
    }

    /**
     * Create an {@link AsyncLoadingTask} based on an {@link FDBDatabaseRunner} and a retriable operation. On
     * {@link #load()}, this will run the operation (perhaps more than once if retries are required). On
     * {@link #close()}, this will close the runner, which should cancel any ongoing transactions and stop
     * future tasks from running.
     *
     * @param runner the runner to use to run the retriable task
     * @param retriable the retriable operation to run
     * @param <T> the type returned by the retriable task
     * @return a task that wraps running the task with an {@link FDBDatabaseRunner}
     */
    static <T> AsyncLoadingTask<T> fromDatabaseRunner(@Nonnull FDBDatabaseRunner runner, @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return new AsyncLoadingTask<T>() {
            @Override
            public CompletableFuture<T> load() {
                return runner.runAsync(retriable);
            }

            @Override
            public void close() {
                runner.close();
            }
        };
    }
}

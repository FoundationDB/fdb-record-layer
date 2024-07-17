/*
 * MoreAsyncUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.async;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.util.LoggableException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.apple.foundationdb.async.AsyncUtil.collect;
import static com.apple.foundationdb.async.AsyncUtil.tag;
import static com.apple.foundationdb.async.AsyncUtil.whenAny;
import static com.apple.foundationdb.async.AsyncUtil.whileTrue;

/**
 * More helpers in the spirit of {@link AsyncUtil}.
 */
@API(API.Status.UNSTABLE)
public class MoreAsyncUtil {

    private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor
            = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setDaemon(true).build());

    static {
        scheduledThreadPoolExecutor.setKeepAliveTime(30, TimeUnit.SECONDS);
        scheduledThreadPoolExecutor.allowCoreThreadTimeOut(true);
    }

    @Nonnull
    public static <T> AsyncIterable<T> limitIterable(@Nonnull final AsyncIterable<T> iterable,
                                                     final int limit) {
        return new AsyncIterable<T>() {
            @Nonnull
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    final AsyncIterator<T> iterator = iterable.iterator();
                    int count = 0;

                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (count < limit) {
                            return iterator.onHasNext();
                        } else {
                            return AsyncUtil.READY_FALSE;
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return (count < limit) && iterator.hasNext();
                    }

                    @Override
                    public T next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        count++;
                        return iterator.next();
                    }

                    @Override
                    public void close() {
                        closeIterator(iterator);
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }

            @Override
            public CompletableFuture<List<T>> asList() {
                return collect(this);
            }
        };
    }

    /**
     * Filter items from an async iterable.
     * @param iterable the source
     * @param filter only items in iterable for which this function returns true will appear in the return value
     * @param <T> the source type
     * @return a new {@code AsyncIterable} that only contains those items in iterable for which filter returns {@code true}
     */
    @Nonnull
    public static <T> AsyncIterable<T> filterIterable(@Nonnull final AsyncIterable<T> iterable,
                                                      @Nonnull final Function<T, Boolean> filter) {
        return filterIterable(ForkJoinPool.commonPool(), iterable, filter);
    }

    @Nonnull
    public static <T> AsyncIterable<T> filterIterable(@Nonnull Executor executor,
                                                      @Nonnull final AsyncIterable<T> iterable,
                                                      @Nonnull final Function<T, Boolean> filter) {
        return new AsyncIterable<T>() {
            @Nonnull
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    final AsyncIterator<T> iterator = iterable.iterator();
                    T next;
                    boolean haveNext;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

                    @Nonnull
                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (nextFuture != null) {
                            return nextFuture;
                        }
                        if (haveNext) {
                            return AsyncUtil.READY_TRUE;
                        }
                        nextFuture = whileTrue(() -> iterator.onHasNext()
                            .thenApply(hasNext -> {
                                if (!hasNext) {
                                    return false;
                                }
                                next = iterator.next();
                                haveNext = filter.apply(next);
                                return !haveNext;
                            }), executor)
                            .thenApply(v -> haveNext);
                        return nextFuture;
                    }

                    @Override
                    public boolean hasNext() {
                        if (nextFuture != null) {
                            nextFuture.join();
                            nextFuture = null;
                        }
                        while (!haveNext && iterator.hasNext()) {
                            next = iterator.next();
                            haveNext = filter.apply(next);
                        }
                        return haveNext;
                    }

                    @Override
                    public T next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        haveNext = false;
                        return next;
                    }

                    @Override
                    public void close() {
                        if (nextFuture != null) {
                            nextFuture.cancel(false);
                            nextFuture = null;
                        }
                        closeIterator(iterator);
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }

            @Override
            public CompletableFuture<List<T>> asList() {
                return collect(this, executor);
            }
        };
    }

    /**
     * Remove adjacent duplicates form iterable.
     * Note: if iterable is sorted, this will actually remove duplicates.
     * @param iterable the source
     * @param <T> the source type
     * @return a new {@code AsyncIterable} that only contains those items in iterable for which the previous item was different
     */
    @Nonnull
    public static <T> AsyncIterable<T> dedupIterable(@Nonnull final AsyncIterable<T> iterable) {
        return dedupIterable(ForkJoinPool.commonPool(), iterable);
    }

    @Nonnull
    public static <T> AsyncIterable<T> dedupIterable(@Nonnull Executor executor,
                                                     @Nonnull final AsyncIterable<T> iterable) {
        return filterIterable(executor, iterable,
                new Function<T, Boolean>() {
                    private Object lastObj;

                    @Nonnull
                    @Override
                    public Boolean apply(T obj) {
                        if ((lastObj != null) && lastObj.equals(obj)) {
                            return false;
                        } else {
                            lastObj = obj;
                            return true;
                        }
                    }
                });
    }

    /**
     * Create a new iterable that has the contents of all the parameters in order.
     * @param iterables a list of iterables to concatenate together
     * @param <T> the source type
     * @return a new {@code AsyncIterable} that starts with all the elements of the first iterable provided,
     * then all the elements of the second iterable and so on
     */
    @Nonnull
    @SuppressWarnings("unchecked") // parameterized vararg
    public static <T> AsyncIterable<T> concatIterables(@Nonnull final AsyncIterable<T>... iterables) {
        return concatIterables(ForkJoinPool.commonPool(), iterables);
    }

    @Nonnull
    @SuppressWarnings("unchecked") // parameterized vararg
    public static <T> AsyncIterable<T> concatIterables(@Nonnull Executor executor, @Nonnull final AsyncIterable<T>... iterables) {
        return new AsyncIterable<T>() {
            @Nonnull
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    int index = 0;
                    @Nullable
                    AsyncIterator<T> current;
                    AsyncIterator<T> removeFrom;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

                    @Nonnull
                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (nextFuture != null) {
                            return nextFuture;
                        }
                        if (index >= iterables.length) {
                            return AsyncUtil.READY_FALSE;
                        }
                        nextFuture = whileTrue(() -> {
                            if (current == null) {
                                current = iterables[index].iterator();
                            }
                            return current.onHasNext()
                                    .thenApply(hasNext -> {
                                        if (hasNext) {
                                            return false;
                                        } else {
                                            current = null;
                                            return (++index < iterables.length);
                                        }
                                    });
                        }, executor).thenApply(v -> (index < iterables.length));
                        return nextFuture;
                    }

                    @Override
                    public boolean hasNext() {
                        if (nextFuture != null) {
                            nextFuture.join();
                            nextFuture = null;
                        }
                        while (index < iterables.length) {
                            if (current == null) {
                                current = iterables[index].iterator();
                            }
                            if (current.hasNext()) {
                                return true;
                            }
                            current = null;
                            index++;
                        }
                        return false;
                    }

                    @Override
                    public T next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        removeFrom = current;
                        return current.next();
                    }

                    @Override
                    public void close() {
                        if (nextFuture != null) {
                            nextFuture.cancel(false);
                            nextFuture = null;
                        }
                        closeIterator(current);
                    }

                    @Override
                    public void remove() {
                        if (removeFrom != null) {
                            removeFrom.remove();
                            removeFrom = null;
                        } else {
                            throw new IllegalStateException("Nothing to remove");
                        }
                    }
                };
            }

            @Override
            public CompletableFuture<List<T>> asList() {
                if (iterables.length == 0) {
                    return CompletableFuture.completedFuture(Collections.<T>emptyList());
                } else if (iterables.length == 1) {
                    return iterables[0].asList();
                } else {
                    final List<T> result = new ArrayList<>();
                    return tag(whileTrue(new Supplier<CompletableFuture<Boolean>>() {
                                int index = 0;

                                @Override
                                public CompletableFuture<Boolean> get() {
                                    return iterables[index++].asList()
                                            .thenApply(asList -> {
                                                result.addAll(asList);
                                                return (index < iterables.length);
                                            });
                                }
                            }, executor),
                            result);
                }
            }
        };
    }

    /**
     * Maps each value in an iterable to a new iterable and returns the concatenated results.
     * This will start a pipeline of asynchronous requests
     * for up to a requested number of elements of the iterable, in parallel with requests to the mapping results.
     * This does not pipeline the overlapping concatenations, i.e. it won't grab the first item of the
     * second result of func, until it has exhausted the first result of func.
     * @param iterable the source
     * @param func mapping function from each element of iterable to a new iterable
     * @param pipelineSize the number of elements to pipeline
     * @param <T1> the type of the source
     * @param <T2> the type of the destination iterables
     * @return the results of all the {@code AsyncIterable}s returned by func for each value of iterable, concatenated
     */
    @Nonnull
    public static <T1, T2> AsyncIterable<T2> mapConcatIterable(@Nonnull final AsyncIterable<T1> iterable,
                                                               @Nonnull final Function<T1, AsyncIterable<T2>> func,
                                                               final int pipelineSize) {
        return mapConcatIterable(ForkJoinPool.commonPool(), iterable, func, pipelineSize);
    }

    @Nonnull
    public static <T1, T2> AsyncIterable<T2> mapConcatIterable(@Nonnull Executor executor,
                                                               @Nonnull final AsyncIterable<T1> iterable,
                                                               @Nonnull final Function<T1, AsyncIterable<T2>> func,
                                                               final int pipelineSize) {
        return new AsyncIterable<T2>() {
            @Nonnull
            @Override
            public CloseableAsyncIterator<T2> iterator() {
                CloseableAsyncIterator<T2> it = new CloseableAsyncIterator<T2>() {
                    final AsyncIterator<T1> iterator = iterable.iterator();
                    final Queue<AsyncIterator<T2>> pipeline = new ArrayDeque<>(pipelineSize);
                    @Nullable
                    AsyncIterator<T2> removeFrom;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

                    @Nonnull
                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (nextFuture != null) {
                            return nextFuture;
                        }
                        nextFuture = whileTrue(() -> {
                            List<CompletableFuture<Boolean>> waitOn = new ArrayList<>(2);
                            CompletableFuture<Boolean> outer = iterator.onHasNext();
                            if (isCompletedNormally(outer)) {
                                if (outer.getNow(false) && (pipeline.size() < pipelineSize)) {
                                    AsyncIterator<T2> next = func.apply(iterator.next()).iterator();
                                    pipeline.add(next);
                                    next.onHasNext();
                                    return AsyncUtil.READY_TRUE; // return to the top of this whileTrue
                                }
                            } else {
                                waitOn.add(outer);
                            }

                            CompletableFuture<Boolean> inner;
                            AsyncIterator<T2> current = pipeline.peek();
                            if (current != null) {
                                inner = current.onHasNext();
                                if (isCompletedNormally(inner)) {
                                    if (inner.getNow(false)) {
                                        // inner onHasNext returned true, break out of whileTrue
                                        return AsyncUtil.READY_FALSE; // First available
                                    } else {
                                        // inner exhausted, return to top of this whileTrue
                                        pipeline.remove();
                                        return AsyncUtil.READY_TRUE;
                                    }
                                } else {
                                    waitOn.add(inner);
                                }
                            }
                            // TODO whenAny should special handle elements of 1
                            if (waitOn.size() == 1) {
                                return waitOn.get(0).thenApply(new AlwaysTrue<>());
                            } else {
                                return whenAny(waitOn).thenApply(new AlwaysTrue<>());
                            }
                        }, executor).thenApply(v -> !pipeline.isEmpty());
                        return nextFuture;
                    }

                    @Override
                    public boolean hasNext() {
                        // Always keep the pipeline full, even when called synchronously.
                        return onHasNext().join();
                    }

                    @Override
                    public T2 next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        nextFuture = null;
                        AsyncIterator<T2> current = pipeline.peek();
                        removeFrom = current;
                        return current.next();
                    }

                    @Override
                    public void close() {
                        if (nextFuture != null) {
                            nextFuture.cancel(false);
                            nextFuture = null;
                        }
                        for (AsyncIterator<T2> pending : pipeline) {
                            closeIterator(pending);
                        }
                        closeIterator(iterator);
                    }

                    @Override
                    public void remove() {
                        if (removeFrom != null) {
                            removeFrom.remove();
                            removeFrom = null;
                        } else {
                            throw new IllegalStateException("Nothing to remove");
                        }
                    }
                };
                it.onHasNext(); // Initial pipeline fill.
                return it;
            }

            @Override
            public CompletableFuture<List<T2>> asList() {
                final List<T2> result = new ArrayList<>();
                return tag(whileTrue(new Supplier<CompletableFuture<Boolean>>() {
                            final AsyncIterator<T1> iterator = iterable.iterator();
                            boolean more = false;

                            @Override
                            public CompletableFuture<Boolean> get() {
                                if (more) {
                                    more = false;
                                    return func.apply(iterator.next()).asList()
                                            .thenApply(items -> {
                                                result.addAll(items);
                                                return true;
                                            });
                                } else {
                                    more = true;
                                    return iterator.onHasNext();
                                }
                            }
                        }, executor),
                        result);
            }
        };
    }

    // Filtering and mapping implemented using general pipelined
    // fan-out.  These could be implemented slightly more efficiently,
    // but then they'd have to duplicate the pipeline logic.

    /**
     * Filters a single item, returning an {@link AsyncIterable} of either 0 elements or just the provided one.
     * If the filter returns a {@code true} future, the resulting {@code AsyncIterable} will have the given item,
     * otherwise it will be empty.
     * @param item an item to potentially be filtered
     * @param filter a function that returns an asynchronous future to determine whether or not
     * to return item
     * @return an {@code AsyncIterable} that will either contain item or nothing, depending on the result
     * of filter
     */
    @Nonnull
    static <T> AsyncIterable<T> filterToIterable(final T item,
                                                 @Nonnull final Function<T, CompletableFuture<Boolean>> filter) {
        return new AsyncIterable<T>() {
            @Nullable
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    boolean used = false;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

                    @Nullable
                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (used) {
                            return AsyncUtil.READY_FALSE;
                        }
                        if (nextFuture == null) {
                            nextFuture = filter.apply(item);
                        }
                        return nextFuture;
                    }

                    @Override
                    public boolean hasNext() {
                        if (used) {
                            return false;
                        }
                        if (nextFuture != null) {
                            return nextFuture.join();
                        } else {
                            return filter.apply(item).join();
                        }
                    }

                    @Override
                    public T next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        used = true;
                        return item;
                    }

                    @Override
                    public void close() {
                        if (nextFuture != null) {
                            nextFuture.cancel(false);
                            nextFuture = null;
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public CompletableFuture<List<T>> asList() {
                return filter.apply(item)
                    .thenApply((Function<Boolean, List<T>>) match -> match ? Collections.singletonList(item) : Collections.emptyList());
            }
        };
    }

    /**
     * Filter an iterable, pipelining the asynchronous filter functions.
     * Unlike filterIterable, the filter here is asynchronous.
     * As items comes back from iterable, a pipeline of filter futures is kept without advancing
     * the iterable.
     * @param iterable the source
     * @param filter only the values of iterable for which the future returned by this filter returns true
     * will be in the resulting iterable
     * @param pipelineSize the number of filter results to pipeline
     * @param <T> the source type
     * @return a new {@code AsyncIterable} containing the elements of iterable for which filter returns a true future
     */
    @Nonnull
    public static <T> AsyncIterable<T> filterIterablePipelined(@Nonnull AsyncIterable<T> iterable,
                                                               @Nonnull final Function<T, CompletableFuture<Boolean>> filter,
                                                               int pipelineSize) {
        return filterIterablePipelined(ForkJoinPool.commonPool(), iterable, filter, pipelineSize);
    }

    @Nonnull
    public static <T> AsyncIterable<T> filterIterablePipelined(@Nonnull Executor executor,
                                                               @Nonnull AsyncIterable<T> iterable,
                                                               @Nonnull final Function<T, CompletableFuture<Boolean>> filter,
                                                               int pipelineSize) {
        return mapConcatIterable(iterable,
                item -> filterToIterable(item, filter),
                pipelineSize);
    }

    /**
     * Converts a single item to an iterable of a different type.
     * @param item the source
     * @param func asynchronously map item to a new type
     * @param <T1> the source type
     * @param <T2> the destination type
     * @return a new {@code AsyncIterable} containing the result of func(item)
     */
    @Nonnull
    static <T1, T2> AsyncIterable<T2> mapToIterable(final T1 item,
                                                    @Nonnull final Function<T1, CompletableFuture<T2>> func) {
        return new AsyncIterable<T2>() {
            @Nullable
            @Override
            public CloseableAsyncIterator<T2> iterator() {
                return new CloseableAsyncIterator<T2>() {
                    T2 result;
                    boolean used = false;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

                    @Nullable
                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (used) {
                            return AsyncUtil.READY_FALSE;
                        }
                        if (nextFuture == null) {
                            nextFuture = func.apply(item)
                                .thenApply(r -> {
                                    result = r;
                                    return true;
                                });
                        }
                        return nextFuture;
                    }

                    @Override
                    public boolean hasNext() {
                        return !used;
                    }

                    @Override
                    public T2 next() {
                        if (used) {
                            throw new NoSuchElementException();
                        }
                        if (nextFuture != null) {
                            nextFuture.join();
                        } else {
                            result = func.apply(item).join();
                        }
                        used = true;
                        return result;
                    }

                    @Override
                    public void close() {
                        if (nextFuture != null) {
                            nextFuture.cancel(false);
                            nextFuture = null;
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public CompletableFuture<List<T2>> asList() {
                return func.apply(item)
                    .thenApply(result -> Collections.singletonList(result));
            }
        };
    }

    /**
     * Maps an AsyncIterable using an asynchronous mapping function.
     * @param iterable the source
     * @param func Maps items of iterable to a new value asynchronously
     * @param pipelineSize the number of map results to pipeline. As items comes back from iterable,
     * this will have up to this many func futures in waiting before waiting on them without advancing
     * the iterable.
     * @param <T1> the source type
     * @param <T2> the destination type
     * @return a new {@code AsyncIterable} with the results of applying func to each of the elements of iterable
     */
    @Nonnull
    public static <T1, T2> AsyncIterable<T2> mapIterablePipelined(@Nonnull AsyncIterable<T1> iterable,
                                                                  @Nonnull final Function<T1, CompletableFuture<T2>> func,
                                                                  int pipelineSize) {
        return mapConcatIterable(iterable,
                item -> mapToIterable(item, func),
                pipelineSize);
    }

    /**
     * A holder for a (mutable) value.
     * @param <T> type of value to hold
     */
    public static class Holder<T> {
        public T value;

        public Holder(T value) {
            this.value = value;
        }
    }

    /**
     * Reduce contents of iterator to single value.
     * @param iterator source of values
     * @param identity initial value for reduction
     * @param accumulator function that takes previous reduced value and computes new value combining iterator element
     * @param <U> the result type of the reduction
     * @param <T> the element type of the iterator
     * @return the reduced result
     */
    @Nullable
    public static <U, T> CompletableFuture<U> reduce(@Nonnull AsyncIterator<T> iterator, U identity,
                                                     BiFunction<U, ? super T, U> accumulator) {
        return reduce(ForkJoinPool.commonPool(), iterator, identity, accumulator);
    }

    @Nullable
    public static <U, T> CompletableFuture<U> reduce(@Nonnull Executor executor,
                                                     @Nonnull AsyncIterator<T> iterator, U identity,
                                                     BiFunction<U, ? super T, U> accumulator) {
        Holder<U> holder = new Holder<>(identity);
        return whileTrue(() -> iterator.onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                holder.value = accumulator.apply(holder.value, iterator.next());
            }
            return hasNext;
        }), executor).thenApply(vignore -> holder.value);
    }

    /**
     * Returns whether the given {@link CompletableFuture} has completed normally, i.e., not exceptionally.
     * If the future is yet to complete or if the future completed with an error, then this
     * will return <code>false</code>.
     * @param future the future to check for normal completion
     * @return whether the future has completed without exception
     */
    @API(API.Status.MAINTAINED)
    public static boolean isCompletedNormally(@Nonnull CompletableFuture<?> future) {
        return future.isDone() && !future.isCompletedExceptionally();
    }

    /**
     * Creates a future that will be ready after the given delay. Creating the delayed future does
     * not use more than one thread for all of the futures together, and it is safe to create many delayed
     * futures at once. The guarantee given by this function is that the future will not be ready sooner
     * than the delay specified. It may, however, fire after the given delay (especially if there are multiple delayed
     * futures that are trying to fire at once).
     *
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a {@link CompletableFuture} that will fire after the given delay
     */
    @API(API.Status.MAINTAINED)
    @Nonnull
    public static CompletableFuture<Void> delayedFuture(long delay, @Nonnull TimeUnit unit) {
        if (delay <= 0) {
            return AsyncUtil.DONE;
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduledThreadPoolExecutor.schedule(() -> future.complete(null), delay, unit);
        return future;
    }

    /**
     * Get a completable future that will either complete within the specified deadline time or complete exceptionally
     * with {@link DeadlineExceededException}. If {@code deadlineTimeMillis} is set to {@link Long#MAX_VALUE}, then
     * no deadline is imposed on the future.
     *
     * @param deadlineTimeMillis the maximum time to wait for the asynchronous operation to complete, specified in milliseconds
     * @param supplier the {@link Supplier} of the asynchronous result
     * @param <T> the return type for the get operation
     * @return a future that will either complete with the result of the asynchronous get operation or
     * complete exceptionally if the deadline is exceeded
     */
    @API(API.Status.EXPERIMENTAL)
    public static <T> CompletableFuture<T> getWithDeadline(long deadlineTimeMillis,
                                                           @Nonnull Supplier<CompletableFuture<T>> supplier) {
        final CompletableFuture<T> valueFuture = supplier.get();
        if (deadlineTimeMillis == Long.MAX_VALUE) {
            return valueFuture;
        }
        return CompletableFuture.anyOf(MoreAsyncUtil.delayedFuture(deadlineTimeMillis, TimeUnit.MILLISECONDS), valueFuture)
                .thenCompose(ignore -> {
                    if (!valueFuture.isDone()) {
                        // if the future is not ready then we exceeded the timeout
                        valueFuture.completeExceptionally(new DeadlineExceededException(deadlineTimeMillis));
                    }
                    return valueFuture;
                });
    }

    /**
     * Close the given iterator, or at least cancel it.
     * @param iterator iterator to close
     */
    @API(API.Status.MAINTAINED)
    public static void closeIterator(@Nonnull Iterator<?> iterator) {
        if (iterator instanceof CloseableAsyncIterator) {
            ((CloseableAsyncIterator<?>)iterator).close();
        } else if (iterator instanceof AsyncIterator) {
            ((AsyncIterator<?>)iterator).cancel();
        } else if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable)iterator).close();
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    /**
     * This is supposed to replicate the semantics of {@link java.util.concurrent.CompletionStage#whenComplete(BiConsumer)}
     * but to handle the case where the completion handler might itself contain async work.
     * @param future future to compose the handler onto
     * @param handler handler bi-function to compose onto the passed future
     * @param exceptionMapper function for mapping the underlying exception to a {@link RuntimeException}
     * @param <V> return type of original future
     * @return future with same completion properties as the future returned by the handler
     * @see #composeWhenCompleteAndHandle(CompletableFuture, BiFunction, Function)
     */
    public static <V> CompletableFuture<V> composeWhenComplete(
            @Nonnull CompletableFuture<V> future,
            @Nonnull BiFunction<V, Throwable, CompletableFuture<Void>> handler,
            @Nullable Function<Throwable, RuntimeException> exceptionMapper) {
        return composeWhenCompleteAndHandle(
                future,
                (result, exception) -> handler.apply(result, exception).thenApply(vignore -> result),
                exceptionMapper);
    }

    /**
     * Compose a handler bi-function to the result of a future. Unlike the
     * {@link AsyncUtil#composeHandle(CompletableFuture, BiFunction)}, which completes exceptionally only when the
     * <code>handler</code> completes exceptionally, it completes exceptionally even if the supplied action itself
     * (<code>future</code>) encounters an exception.
     * @param future future to compose the handler onto
     * @param handler handler bi-function to compose onto the passed future
     * @param exceptionMapper function for mapping the underlying exception to a {@link RuntimeException}
     * @param <V> type of original future
     * @param <T> type of final future
     * @return future with same completion properties as the future returned by the handler
     * @see AsyncUtil#composeHandle(CompletableFuture, BiFunction)
     */
    public static <V, T> CompletableFuture<T> composeWhenCompleteAndHandle(
            @Nonnull CompletableFuture<V> future,
            @Nonnull BiFunction<V, Throwable, ? extends CompletableFuture<T>> handler,
            @Nullable Function<Throwable, RuntimeException> exceptionMapper) {
        return AsyncUtil.composeHandle(future, (futureResult, futureException) -> {
            try {
                return handler.apply(futureResult, futureException).handle((handlerResult, handlerAsyncException) -> {
                    if (futureException != null) {
                        throw getRuntimeException(futureException, exceptionMapper);
                    } else if (handlerAsyncException != null) {
                        // This is for the case where the function call handler.apply returns an exceptional future.
                        throw getRuntimeException(handlerAsyncException, exceptionMapper);
                    } else {
                        return handlerResult;
                    }
                });
            } catch (Exception handlerSyncException) {
                // This is for the case where the function call handler.apply throws an error.
                throw getRuntimeException(handlerSyncException, exceptionMapper);
            }
        });
    }

    /**
     * Handle when <code>futureSupplier</code> encounters an exception when supplying a future, or the future is completed
     * exceptionally. Unlike the "handle" in CompletableFuture, <code>handlerOnException</code> is not executed if
     * the future is successful.
     * @param futureSupplier the supplier of future which needs to be handled
     * @param handlerOnException the handler when the future encounters an exception
     * @param <V> the result type of the future
     * @return future that completes exceptionally if the handler has exception
     */
    public static <V> CompletableFuture<V> handleOnException(Supplier<CompletableFuture<V>> futureSupplier,
                                                             Function<Throwable, CompletableFuture<V>> handlerOnException) {
        try {
            return AsyncUtil.composeHandle(futureSupplier.get(), (futureResult, futureException) -> {
                if (futureException != null) {
                    // This is for the case where future completes exceptionally
                    return handlerOnException.apply(futureException);
                } else {
                    return CompletableFuture.completedFuture(futureResult);
                }
            });
        } catch (Exception e) {
            // This is for the case where futureSupplier.get() throws an error.
            return handlerOnException.apply(e);
        }
    }

    private static RuntimeException getRuntimeException(@Nonnull Throwable exception,
                                                        @Nullable Function<Throwable, RuntimeException> exceptionMapper) {
        return exceptionMapper == null ? new RuntimeException(exception) : exceptionMapper.apply(exception);
    }

    /**
     * Combine the results of two futures, but fail fast if either future fails.
     * <p>
     *     This has the same behavior as {@link CompletableFuture#thenCombine}, except, if either future fails, it won't
     *     wait for the other one to complete before completing the result with the failure.
     * </p>
     * @param future1 one future
     * @param future2 another future
     * @param combiner a function to combine the results of both {@code future1} and {@code future2} into a single result.
     *
     * @param <T> the result type for {@code future1}
     * @param <U> the result type for {@code future2}
     * @param <R> the result type for the returned future
     *
     * @return a future that fails with one of the exceptions from {@code future1} or {@code future2} if either of those
     * failed, or the result of applying {@code combiner} to their results if both succeeded.
     */
    public static <T, U, R> CompletableFuture<R> combineAndFailFast(CompletableFuture<T> future1,
                                                                    CompletableFuture<U> future2,
                                                                    BiFunction<T, U, R> combiner) {
        // The lambda called within thenCompose is only called if one of the futures has succeeded, at which point
        // we can use thenCombine.
        // If neither has succeeded (yet) but one fails, then the anyOf future will complete exceptionally and
        // thenCompose will not be executed.
        return CompletableFuture.anyOf(future1, future2)
                .thenCompose(vignore -> future1.thenCombine(future2, combiner));
    }

    /**
     * A {@code Boolean} function that is always true.
     * @param <T> the type of the (ignored) argument to the function
     */
    public static class AlwaysTrue<T> implements Function<T, Boolean> {

        @Nonnull
        @Override
        public Boolean apply(T t) {
            return true;
        }
    }

    /**
     * This is a static class, and should not be instantiated.
     **/
    private MoreAsyncUtil() {
    }

    /**
     * Exception that will be thrown when the <code>supplier</code> in {@link #getWithDeadline(long, Supplier)} fails to
     * complete within the specified deadline time.
     */
    @SuppressWarnings("serial")
    public static class DeadlineExceededException extends LoggableException {
        private DeadlineExceededException(long deadlineTimeMillis) {
            super("deadline exceeded");
            addLogInfo("deadlineTimeMillis", deadlineTimeMillis);
        }
    }
}

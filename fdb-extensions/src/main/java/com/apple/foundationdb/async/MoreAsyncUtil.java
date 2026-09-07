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
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jspecify.annotations.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
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

    private static final Supplier<ScheduledThreadPoolExecutor> scheduledExecutorSupplier = Suppliers.memoize(() -> {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("fdb-scheduled-executor-%d")
                .build();
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, threadFactory);
        scheduledThreadPoolExecutor.setKeepAliveTime(30, TimeUnit.SECONDS);
        scheduledThreadPoolExecutor.allowCoreThreadTimeOut(true);
        return scheduledThreadPoolExecutor;
    });

    public static <T> CompletableFuture<T> alreadyCancelled() {
        final CompletableFuture<T> alreadyCancelled = new CompletableFuture<>();
        alreadyCancelled.cancel(false);
        return alreadyCancelled;
    }

    public static <T> AsyncIterable<T> iterableOf(final Supplier<AsyncIterator<T>> iteratorSupplier,
                                                  final Executor executor) {
        return new AsyncIterable<>() {
            @Override
            public AsyncIterator<T> iterator() {
                return iteratorSupplier.get();
            }

            @Override
            public CompletableFuture<List<T>> asList() {
                return collect(this, executor);
            }
        };
    }

    /**
     * Drives an {@link AsyncIterable} to exhaustion for its side effects, discarding every element. Useful when the
     * pipeline producing the iterable is lazy (each stage runs only as its output is pulled) and the caller cares
     * about the work done while iterating -- writes, counter updates, statistics accumulation -- rather than about the
     * values themselves. Equivalent to {@link #consumeRemaining} over a fresh iterator of {@code iterable}.
     *
     * @param iterable the iterable to drain
     * @param executor the executor to advance the iteration on
     * @param <T> the element type (elements are discarded)
     *
     * @return a future that completes with {@code null} once the iterable is exhausted, or exceptionally if advancing
     *         it fails
     */
    public static <T> CompletableFuture<Void> consume(final AsyncIterable<T> iterable,
                                                      final Executor executor) {
        return consumeRemaining(iterable.iterator(), executor);
    }

    /**
     * Drives the remaining elements of an {@link AsyncIterator} to exhaustion for their side effects, discarding each
     * one. Iteration starts from the iterator's current position, so a partially consumed iterator is drained the rest
     * of the way. Use {@link #consume} to drain an entire {@link AsyncIterable} from the beginning.
     *
     * @param iterator the iterator to drain from its current position
     * @param executor the executor to advance the iteration on
     * @param <T> the element type (elements are discarded)
     *
     * @return a future that completes with {@code null} once the iterator is exhausted, or exceptionally if advancing
     *         it fails
     */
    public static <T> CompletableFuture<Void> consumeRemaining(final AsyncIterator<T> iterator,
                                                               final Executor executor) {
        // tag() is from the unannotated fdb-java client library, so its generic value parameter is treated as
        // @NonNull by NullAway's defaults; Void's only inhabitant is null, so this is the correct call.
        @SuppressWarnings("NullAway")
        final CompletableFuture<Void> result = tag(AsyncUtil.forEachRemaining(iterator, t -> { }, executor), null);
        return result;
    }

    public static <T> AsyncIterable<T> limitIterable(final AsyncIterable<T> iterable,
                                                     final int limit, final Executor executor) {
        return iterableOf(() -> limitRemaining(iterable.iterator(), limit), executor);
    }

    public static <T> CloseableAsyncIterator<T> limitRemaining(final AsyncIterator<T> iterator,
                                                               final int limit) {
        return new CloseableAsyncIterator<T>() {
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
                return onHasNext().join();
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

    /**
     * Returns an {@link AsyncIterable} over the longest <em>prefix</em> of {@code iterable} whose elements all satisfy
     * {@code whilePredicate}, stopping at (and excluding) the first element that fails. For example, taking-while
     * {@code isEven} over {@code [2, 4, 3, 6, 5]} yields {@code [2, 4]}.
     *
     * @param iterable the source
     * @param whilePredicate the predicate an element must satisfy to be included; the first failure ends the stream
     * @param executor the executor used to drive the asynchronous iteration
     * @param <T> the element type
     *
     * @return an {@link AsyncIterable} over the leading run of matching elements
     */
    public static <T> AsyncIterable<T> takeWhileIterable(final AsyncIterable<T> iterable,
                                                         final Predicate<T> whilePredicate,
                                                         final Executor executor) {
        return iterableOf(() -> takeWhileRemaining(iterable.iterator(), whilePredicate), executor);
    }

    /**
     * Returns a {@link CloseableAsyncIterator} over the longest <em>prefix</em> of {@code iterator} whose remaining
     * elements all satisfy {@code whilePredicate}: it yields every leading element that matches and stops as soon as
     * one does not (that first failing element is not returned). For example, taking-while {@code isEven} over
     * {@code [2, 4, 3, 6, 5]} yields {@code [2, 4]}.
     * <p>
     * <b>Note on a shared source iterator.</b> Because {@link AsyncIterator} offers no {@code peek}, deciding where to
     * stop requires actually pulling the first non-matching element ({@code iterator.next()}) in order to test it —
     * that element is then dropped rather than returned. A caller that retains a reference to the source
     * {@code iterator} will therefore find it positioned <em>past</em> the first failure: in the example above,
     * {@code iterator.next()} on the original iterator would yield {@code 6}, not {@code 3}. In other words this
     * consumes one element beyond the returned prefix, so sharing the source iterator across a {@code takeWhile} and
     * other consumption is best avoided.
     *
     * @param iterator the source iterator; consumed up to and including the first non-matching element
     * @param whilePredicate the predicate an element must satisfy to be included; the first failure ends the stream
     * @param <T> the element type
     *
     * @return a {@link CloseableAsyncIterator} over the leading run of matching elements
     */
    public static <T> CloseableAsyncIterator<T> takeWhileRemaining(final AsyncIterator<T> iterator,
                                                                   final Predicate<T> whilePredicate) {
        return new CloseableAsyncIterator<>() {
            boolean done = false;
            @Nullable
            CompletableFuture<Boolean> nextFuture = null;
            @Nullable
            T next = null;

            @Override
            public CompletableFuture<Boolean> onHasNext() {
                if (nextFuture == null) {
                    if (!done) {
                        nextFuture = iterator.onHasNext().thenApply(this::advanceIfMatches);
                    } else {
                        nextFuture = AsyncUtil.READY_FALSE;
                    }
                }
                return nextFuture;
            }

            /**
             * Maps the inner iterator's {@code onHasNext} result: when there is a next item that still satisfies the
             * predicate, stashes it and reports {@code true}; otherwise marks the stream done and reports {@code false}.
             */
            private boolean advanceIfMatches(final boolean hasNext) {
                if (!hasNext) {
                    done = true;
                    return false;
                }
                final T potentiallyNextItem = iterator.next();
                if (whilePredicate.test(potentiallyNextItem)) {
                    next = potentiallyNextItem;
                    return true;
                }
                done = true;
                return false;
            }

            @Override
            public boolean hasNext() {
                return onHasNext().join();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                // hasNext() (via advanceIfMatches) is guaranteed to have set next when it returns true.
                final T result = Objects.requireNonNull(next);
                nextFuture = null;
                next = null;
                return result;
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

    /**
     * Filter items from an async iterable.
     * @param iterable the source
     * @param filter only items in iterable for which this function returns true will appear in the return value
     * @param <T> the source type
     * @return a new {@code AsyncIterable} that only contains those items in iterable for which filter returns {@code true}
     */
    public static <T extends @Nullable Object> AsyncIterable<T> filterIterable(final AsyncIterable<T> iterable,
                                                      final Function<T, Boolean> filter) {
        return filterIterable(ForkJoinPool.commonPool(), iterable, filter);
    }

    public static <T extends @Nullable Object> AsyncIterable<T> filterIterable(final Executor executor,
                                                      final AsyncIterable<T> iterable,
                                                      final Function<T, Boolean> filter) {
        return iterableOf(() -> filterRemaining(executor, iterable.iterator(), filter), executor);
    }

    public static <T extends @Nullable Object> CloseableAsyncIterator<T> filterRemaining(Executor executor,
                                                                final AsyncIterator<T> iterator,
                                                                final Function<T, Boolean> filter) {
        return new CloseableAsyncIterator<T>() {
            @Nullable
            T next;
            boolean haveNext;
            @Nullable
            CompletableFuture<Boolean> nextFuture;

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
            // hasNext() is guaranteed to have set next when it returns true. Unlike a "missing value" sentinel,
            // next may itself legitimately be null when T is instantiated with a nullable type (e.g.
            // dedupIterable() below is used over streams that can contain null elements). AsyncIterator.next()
            // is unannotated external code that NullAway infers as @NonNull, so this override can't be
            // re-declared @Nullable; Objects.requireNonNull() would be wrong too, since it would incorrectly
            // reject that legitimate null case, so the method is suppressed instead.
            @SuppressWarnings("NullAway")
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

    /**
     * Remove adjacent duplicates form iterable.
     * Note: if iterable is sorted, this will actually remove duplicates.
     * @param iterable the source
     * @param <T> the source type
     * @return a new {@code AsyncIterable} that only contains those items in iterable for which the previous item was different
     */
    public static <T extends @Nullable Object> AsyncIterable<T> dedupIterable(final AsyncIterable<T> iterable) {
        return dedupIterable(ForkJoinPool.commonPool(), iterable);
    }

    public static <T extends @Nullable Object> AsyncIterable<T> dedupIterable(Executor executor,
                                                     final AsyncIterable<T> iterable) {
        return filterIterable(executor, iterable,
                new Function<>() {
                    @Nullable
                    private Object lastObj;

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
    @SuppressWarnings("unchecked") // parameterized vararg
    public static <T> AsyncIterable<T> concatIterables(final AsyncIterable<T>... iterables) {
        return concatIterables(ForkJoinPool.commonPool(), iterables);
    }

    @SuppressWarnings("unchecked") // parameterized vararg
    public static <T> AsyncIterable<T> concatIterables(Executor executor, final AsyncIterable<T>... iterables) {
        return new AsyncIterable<T>() {
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    int index = 0;
                    @Nullable
                    AsyncIterator<T> current;
                    @Nullable
                    AsyncIterator<T> removeFrom;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

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
                        // hasNext() is guaranteed to have set current when it returns true.
                        final AsyncIterator<T> currentIterator = Objects.requireNonNull(current);
                        removeFrom = currentIterator;
                        return currentIterator.next();
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
     * @param executor the executor
     * @param iterable the source
     * @param func mapping function from each element of iterable to a new iterable
     * @param pipelineSize the number of elements to pipeline
     * @param <T1> the type of the source
     * @param <T2> the type of the destination iterables
     * @return the results of all the {@code AsyncIterable}s returned by func for each value of iterable, concatenated
     */
    public static <T1, T2> AsyncIterable<T2> mapConcatIterable(Executor executor,
                                                               final AsyncIterable<T1> iterable,
                                                               final Function<T1, AsyncIterable<T2>> func,
                                                               final int pipelineSize) {
        return new AsyncIterable<>() {
            @Override
            public CloseableAsyncIterator<T2> iterator() {
                CloseableAsyncIterator<T2> it = new CloseableAsyncIterator<T2>() {
                    final AsyncIterator<T1> iterator = iterable.iterator();
                    final Queue<AsyncIterator<T2>> pipeline = new ArrayDeque<>(pipelineSize);
                    @Nullable
                    AsyncIterator<T2> removeFrom;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

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
                            if (waitOn.isEmpty()) {
                                return AsyncUtil.READY_FALSE;
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
    public static <T> AsyncIterable<T> filterToIterable(final T item,
                                                        final Function<T, CompletableFuture<Boolean>> filter) {
        return new AsyncIterable<T>() {
            @Override
            public CloseableAsyncIterator<T> iterator() {
                return new CloseableAsyncIterator<T>() {
                    boolean used = false;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

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

    public static <T> AsyncIterable<T> filterIterablePipelined(Executor executor,
                                                               AsyncIterable<T> iterable,
                                                               final Function<T, CompletableFuture<Boolean>> filter,
                                                               int pipelineSize) {
        return mapConcatIterable(executor, iterable,
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
    public static <T1, T2> AsyncIterable<T2> mapToIterable(final T1 item,
                                                           final Function<T1, CompletableFuture<T2>> func) {
        return new AsyncIterable<T2>() {
            @Override
            public CloseableAsyncIterator<T2> iterator() {
                return new CloseableAsyncIterator<T2>() {
                    @Nullable
                    T2 result;
                    boolean used = false;
                    @Nullable
                    CompletableFuture<Boolean> nextFuture;

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
                        // If nextFuture completed normally, or the direct func.apply(item) branch ran, result is set.
                        return Objects.requireNonNull(result);
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
    public static <T1, T2> AsyncIterable<T2> mapIterablePipelined(AsyncIterable<T1> iterable,
                                                                  final Function<T1, CompletableFuture<T2>> func,
                                                                  int pipelineSize) {
        return mapIterablePipelined(ForkJoinPool.commonPool(), iterable, func, pipelineSize);
    }

    /**
     * Maps an AsyncIterable using an asynchronous mapping function.
     * @param executor the executor to use to do the work
     * @param iterable the source
     * @param func Maps items of iterable to a new value asynchronously
     * @param pipelineSize the number of map results to pipeline. As items comes back from iterable,
     * this will have up to this many func futures in waiting before waiting on them without advancing
     * the iterable.
     * @param <T1> the source type
     * @param <T2> the destination type
     * @return a new {@code AsyncIterable} with the results of applying func to each of the elements of iterable
     */
    public static <T1, T2> AsyncIterable<T2> mapIterablePipelined(final Executor executor,
                                                                  final AsyncIterable<T1> iterable,
                                                                  final Function<T1, CompletableFuture<T2>> func,
                                                                  int pipelineSize) {
        return mapConcatIterable(executor, iterable,
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
    public static <U, T> CompletableFuture<U> reduce(AsyncIterator<T> iterator, U identity,
                                                     BiFunction<U, ? super T, U> accumulator) {
        return reduce(ForkJoinPool.commonPool(), iterator, identity, accumulator);
    }

    // The returned CompletableFuture reference is never itself null (it is always a fresh future constructed
    // below); a pre-existing @Nullable annotation here predating this migration incorrectly described the
    // future's resolved value (which may be null if U is nullable-instantiated) rather than the future
    // reference. Corrected rather than propagated by the mechanical jspecify swap.
    public static <U, T> CompletableFuture<U> reduce(Executor executor,
                                                     AsyncIterator<T> iterator, U identity,
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
    @API(API.Status.UNSTABLE)
    public static boolean isCompletedNormally(CompletableFuture<?> future) {
        return future.isDone() && !future.isCompletedExceptionally();
    }

    /**
     * Get the default scheduled executor service. This is used to schedule delayed tasks
     * in an efficient way by {@link #delayedFuture(long, TimeUnit)}. Adopters can provide their
     * own {@link ScheduledExecutorService} by using the overloaded method.
     *
     * <p>
     * By default, the returned executor service is a {@link ScheduledThreadPoolExecutor}
     * with a single thread used for executing delayed tasks. In practice, with a future chain,
     * this should be quick asynchronous callbacks, and blocking in such a callback can
     * block the task thread.
     * </p>
     *
     * @return an executor service that allows for tasks to be efficiently scheduled for later
     * @see #delayedFuture(long, TimeUnit, ScheduledExecutorService)
     */
    public static ScheduledExecutorService getDefaultScheduledExecutor() {
        return scheduledExecutorSupplier.get();
    }

    /**
     * Creates a future that will be ready after the given delay. This uses the
     * {@link #getDefaultScheduledExecutor()} to schedule tasks but is otherwise identical to
     * {@link #delayedFuture(long, TimeUnit, ScheduledExecutorService)}.
     *
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a future that will be ready after the given delay
     * @see #delayedFuture(long, TimeUnit, ScheduledExecutorService)
     */
    @API(API.Status.UNSTABLE)
    public static CompletableFuture<Void> delayedFuture(long delay, TimeUnit unit) {
        return delayedFuture(delay, unit, getDefaultScheduledExecutor());
    }

    /**
     * Creates a future that will be ready after the given delay. The delayed future will be executed
     * using the supplied {@link ScheduledExecutorService} to complete the returned future. Exact
     * performance can depend on the scheduled executor implementation, but it should generally be
     * safe to create many delayed futures at once. The guarantee given by this function is that the future will not be ready sooner
     * than the delay specified. It may, however, fire after the given delay (especially if there are multiple delayed
     * futures that are trying to fire at once).
     *
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @param scheduledExecutor executor service used to complete the returned future and run any same-thread callbacks
     * @return a {@link CompletableFuture} that will fire after the given delay
     */
    @API(API.Status.UNSTABLE)
    public static CompletableFuture<Void> delayedFuture(long delay, TimeUnit unit, ScheduledExecutorService scheduledExecutor) {
        if (delay <= 0) {
            return AsyncUtil.DONE;
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduledExecutor.schedule(() -> future.complete(null), delay, unit);
        return future;
    }

    /**
     * Get a completable future that will either complete within the specified deadline time or complete exceptionally
     * with {@link DeadlineExceededException}. If {@code deadlineTimeMillis} is set to {@link Long#MAX_VALUE}, then
     * no deadline is imposed on the future.
     *
     * @param deadlineTimeMillis the maximum time to wait for the asynchronous operation to complete, specified in milliseconds
     * @param supplier the {@link Supplier} of the asynchronous result
     * @param scheduledExecutor executor used to handle managing the deadline
     * @param <T> the return type for the get operation
     * @return a future that will either complete with the result of the asynchronous get operation or
     * complete exceptionally if the deadline is exceeded
     */
    @API(API.Status.EXPERIMENTAL)
    public static <T> CompletableFuture<T> getWithDeadline(long deadlineTimeMillis,
                                                           Supplier<CompletableFuture<T>> supplier,
                                                           ScheduledExecutorService scheduledExecutor) {
        final CompletableFuture<T> valueFuture = supplier.get();
        if (deadlineTimeMillis == Long.MAX_VALUE) {
            return valueFuture;
        }
        return CompletableFuture.anyOf(MoreAsyncUtil.delayedFuture(deadlineTimeMillis, TimeUnit.MILLISECONDS, scheduledExecutor), valueFuture)
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
    @API(API.Status.UNSTABLE)
    public static void closeIterator(@Nullable Iterator<?> iterator) {
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
            CompletableFuture<V> future,
            BiFunction<V, Throwable, CompletableFuture<Void>> handler,
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
            CompletableFuture<V> future,
            BiFunction<V, Throwable, ? extends CompletableFuture<T>> handler,
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

    private static RuntimeException getRuntimeException(Throwable exception,
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
     * Swallows exceptions matching a given predicate from a future.
     * @param future a future which you expect to potentially throw an exception
     * @param shouldSwallow a predicate on whether to swallow the error from the future. Note that if the future failed
     * with a {@link CompletionException}, {@code swallowException} will also be called with its cause so that you don't
     * need special handling to cover this.
     *
     * @return a future that will complete successfully if  {@code future} completed successfully, <em>or</em> the
     * {@code shouldSwallow} predicate returned {@code true} for the error that {@code future} threw
     */
    public static CompletableFuture<Void> swallowException(CompletableFuture<Void> future,
                                                           Predicate<Throwable> shouldSwallow) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        future.whenComplete((vignore, err) -> {
            if (err == null || shouldSwallow.test(err) ||
                    (err instanceof CompletionException && err.getCause() != null && shouldSwallow.test(err.getCause()))) {
                result.complete(null);
            } else {
                result.completeExceptionally(err);
            }
        });
        return result;
    }

    /**
     * Method that provides the functionality of a for loop, however, in an asynchronous way. The result of this method
     * is a {@link CompletableFuture} that represents the result of the last iteration of the loop body.
     * @param startI an integer analogous to the starting value of a loop variable in a for loop
     * @param startU an object of some type {@code U} that represents some initial state that is passed to the loop's
     *        initial state
     * @param conditionPredicate a predicate on the loop variable that must be true before the next iteration is
     *        entered; analogous to the condition in a for loop
     * @param stepFunction a unary operator used for modifying the loop variable after each iteration
     * @param body a bi-function to be called for each iteration; this function is initially invoked using
     *        {@code startI} and {@code startU}; the result of the body is then passed into the next iterator's body
     *        together with a new value for the loop variable. In this way callers can access state inside an iteration
     *        that was computed in a previous iteration.
     * @param executor the executor
     * @param <U> the type of the result of the body {@link BiFunction}
     * @return a {@link CompletableFuture} containing the result of the last iteration's body invocation.
     */
    public static <U> CompletableFuture<U> forLoop(final int startI, @Nullable final U startU,
                                                   final IntPredicate conditionPredicate,
                                                   final IntUnaryOperator stepFunction,
                                                   final BiFunction<Integer, U, CompletableFuture<U>> body,
                                                   final Executor executor) {
        return forLoop(startI, startU,
                (i, ignored) -> conditionPredicate.test(i),
                stepFunction, body, executor);
    }

    /**
     * Method that provides the functionality of a for loop, however, in an asynchronous way. The result of this method
     * is a {@link CompletableFuture} that represents the result of the last iteration of the loop body.
     * @param startI an integer analogous to the starting value of a loop variable in a for loop
     * @param startU an object of some type {@code U} that represents some initial state that is passed to the loop's
     *        initial state
     * @param conditionPredicate a predicate on the loop variable that must be true before the next iteration is
     *        entered; analogous to the condition in a for loop
     * @param stepFunction a unary operator used for modifying the loop variable after each iteration
     * @param body a bi-function to be called for each iteration; this function is initially invoked using
     *        {@code startI} and {@code startU}; the result of the body is then passed into the next iterator's body
     *        together with a new value for the loop variable. In this way callers can access state inside an iteration
     *        that was computed in a previous iteration.
     * @param executor the executor
     * @param <U> the type of the result of the body {@link BiFunction}
     * @return a {@link CompletableFuture} containing the result of the last iteration's body invocation.
     */
    public static <U> CompletableFuture<U> forLoop(final int startI, @Nullable final U startU,
                                                   final BiPredicate<Integer, U> conditionPredicate,
                                                   final IntUnaryOperator stepFunction,
                                                   final BiFunction<Integer, U, CompletableFuture<U>> body,
                                                   final Executor executor) {
        final AtomicInteger loopVariableAtomic = new AtomicInteger(startI);
        final AtomicReference<U> lastResultAtomic = new AtomicReference<>(startU);
        return whileTrue(() -> {
            final int loopVariable = loopVariableAtomic.get();
            if (!conditionPredicate.test(loopVariable, lastResultAtomic.get())) {
                return AsyncUtil.READY_FALSE;
            }
            return body.apply(loopVariable, lastResultAtomic.get())
                    .thenApply(result -> {
                        loopVariableAtomic.set(stepFunction.applyAsInt(loopVariable));
                        lastResultAtomic.set(result);
                        return true;
                    });
        }, executor).thenApply(ignored -> lastResultAtomic.get());
    }

    /**
     * Method to iterate over some items, for each of which a body is executed asynchronously. The result of each such
     * executed is then collected in a list and returned as a {@link CompletableFuture} over that list.
     * @param items the items to iterate over
     * @param body a function to be called for each item
     * @param parallelism the maximum degree of parallelism this method should use
     * @param executor the executor
     * @param <T> the type of item
     * @param <U> the type of the result
     * @return a {@link CompletableFuture} containing a list of results collected from the individual body invocations
     */
    @SuppressWarnings({"unchecked", "PMD.CompareObjectsWithEquals"}) // identity check against a null stand-in sentinel
    public static <T, U> CompletableFuture<List<U>> forEach(final Iterable<T> items,
                                                            final Function<T, CompletableFuture<U>> body,
                                                            final int parallelism,
                                                            final Executor executor) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be at least 1, got " + parallelism);
        }
        final Object nullStandIn = new Object();

        // this deque is only modified by once upon creation
        final ArrayDeque<Object> toBeProcessed = new ArrayDeque<>();
        for (final T item : items) {
            toBeProcessed.addLast(item == null ? nullStandIn : item);
        }

        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final AtomicInteger indexAtomic = new AtomicInteger(0);
        final Object[] resultArray = new Object[toBeProcessed.size()];

        return whileTrue(() -> {
            working.removeIf(CompletableFuture::isDone);

            while (working.size() < parallelism) {
                final Object currentObject = toBeProcessed.pollFirst();
                if (currentObject == null) {
                    break;
                }

                final T currentItem =
                        currentObject == nullStandIn ? null : (T)currentObject;

                final int index = indexAtomic.getAndIncrement();
                working.add(body.apply(currentItem)
                        .thenAccept(result -> resultArray[index] = result));
            }

            if (working.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return whenAny(working).thenApply(ignored -> true);
        }, executor).thenApply(ignored -> Arrays.asList((U[])resultArray));
    }

    public static <T> AsyncIterable<T> iterableFromCollection(final CompletableFuture<Collection<T>> collectionFuture,
                                                              final Executor executor) {
        return iterableOf(() -> iteratorFromCollection(collectionFuture), executor);
    }

    public static <T> AsyncIterator<T> iteratorFromCollection(final CompletableFuture<Collection<T>> collectionFuture) {
        return new CloseableAsyncIterator<>() {
            @Nullable
            Iterator<T> iterator = null;

            @Override
            public CompletableFuture<Boolean> onHasNext() {
                if (iterator == null) {
                    return collectionFuture.thenApply(collection -> {
                        this.iterator = collection.iterator();
                        return this.iterator.hasNext();
                    });
                }
                return CompletableFuture.completedFuture(iterator.hasNext());
            }

            @Override
            public boolean hasNext() {
                return onHasNext().join();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return Objects.requireNonNull(iterator).next();
            }

            @Override
            public void close() {
                // nothing
            }
        };
    }

    /**
     * A {@code Boolean} function that is always true.
     * @param <T> the type of the (ignored) argument to the function
     */
    public static class AlwaysTrue<T> implements Function<T, Boolean> {

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
     * Exception that will be thrown when the <code>supplier</code> in {@link #getWithDeadline(long, Supplier, ScheduledExecutorService)} fails to
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

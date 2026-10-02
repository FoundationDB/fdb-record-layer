/*
 * Reducer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Decides what a tertiary cache lookup yields, and what it leaves behind in the cache.
 * <br>
 * A {@link Reducer} is handed the tertiary map whole rather than a pre-selected list of candidates, and the map is
 * the cache's own live view, so an implementation owns every part of the decision: which entries are eligible, which
 * eligible one wins, whether to store a freshly supplied entry, and which metrics that amounts to. An implementation
 * that stores unconditionally is therefore all it takes to write into the cache — see {@link #writeOnly()} — and
 * {@link MultiStageCache} needs no separate {@code put} entry point.
 * <br>
 * Beware that a tertiary key such as {@link PhysicalPlanEquivalence} implements a <em>match</em> relation rather than
 * an equivalence: its {@code hashCode()} is constant and its {@code equals} is asymmetric, evaluating a stored plan
 * constraint against the evaluation context of the key being looked up. An implementation that selects entries must
 * scan rather than call {@link Map#get(Object)}, and must keep the stored key as the receiver of the {@code equals}
 * call.
 *
 * @param <T> the type of the tertiary cache key
 * @param <V> the type of the value stored in the cache
 */
@API(API.Status.EXPERIMENTAL)
public abstract sealed class Reducer<T, V>
        permits Reducer.Matching, Reducer.WriteOnly {

    /**
     * The single {@link WriteOnly} instance. It holds no state of type {@code T} or {@code V} — it only ever writes
     * what the supplier hands it — so one instance serves every parameterisation, in the manner of
     * {@link java.util.Collections#emptyList()}.
     */
    @Nonnull
    private static final Reducer<?, ?> WRITE_ONLY = new WriteOnly<>();

    private Reducer() {
    }

    /**
     * Resolves a lookup of {@code tertiaryKey} against the contents of the tertiary cache.
     *
     * @param tertiaryEntries a live, mutable view of the tertiary cache. Writing to it writes to the cache.
     * @param tertiaryKey the tertiary key being looked up
     * @param tertiaryKeyValueSupplier supplies the key and value to store when the lookup yields nothing
     * @param valueWithEnvironmentDecorator decorates a value found in the cache, preparing it for execution. It is
     *        not applied to a value that came from {@code tertiaryKeyValueSupplier}, which is already current.
     * @param metricCollector collects the events this decision amounts to
     * @return the value the lookup resolves to, never {@code null}
     */
    @Nonnull
    public abstract V reduce(@Nonnull Map<T, V> tertiaryEntries,
                             @Nonnull T tertiaryKey,
                             @Nonnull Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier,
                             @Nonnull Function<V, V> valueWithEnvironmentDecorator,
                             @Nonnull MetricCollector metricCollector);

    /**
     * Stores a freshly supplied entry and returns its value. The value is not passed through the environment
     * decorator: it was just built for this lookup and is already current.
     *
     * @param tertiaryEntries the live view of the tertiary cache to store into
     * @param tertiaryKeyValueSupplier supplies the key and value to store
     * @return the stored value
     */
    @Nonnull
    final V supplyAndStore(@Nonnull final Map<T, V> tertiaryEntries,
                           @Nonnull final Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier) {
        final var keyValuePair = tertiaryKeyValueSupplier.get();
        tertiaryEntries.put(keyValuePair.getKey(), keyValuePair.getValue());
        return keyValuePair.getValue();
    }

    /**
     * Creates a {@link Reducer} that serves a matching entry when there is one, and otherwise stores and returns a
     * freshly supplied entry.
     *
     * @param reductionFunction chooses among the matching entries, and may return {@code null} to reject them all
     * @param <T> the type of the tertiary cache key
     * @param <V> the type of the value stored in the cache
     * @return a read-through {@link Reducer}
     */
    @Nonnull
    public static <T, V> Reducer<T, V> of(@Nonnull final Function<Stream<V>, V> reductionFunction) {
        return new Matching<>(reductionFunction);
    }

    /**
     * Creates a {@link Reducer} that never reads, and always stores and returns a freshly supplied entry.
     * <br>
     * Because it does not select, the stored tertiary keys are never compared and no {@link PhysicalPlanEquivalence}
     * comparison takes place. An entry already held under an equal key is replaced. This is what backs
     * {@code PLAN_CACHE_WRITE_ONLY}: warming the cache without paying for, or being influenced by, a lookup.
     *
     * @param <T> the type of the tertiary cache key
     * @param <V> the type of the value stored in the cache
     * @return the shared write-only {@link Reducer}
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, V> Reducer<T, V> writeOnly() {
        return (Reducer<T, V>)WRITE_ONLY;
    }

    /**
     * The ordinary read-through reducer. See {@link Reducer#of(Function)}.
     *
     * @param <T> the type of the tertiary cache key
     * @param <V> the type of the value stored in the cache
     */
    static final class Matching<T, V> extends Reducer<T, V> {

        @Nonnull
        private final Function<Stream<V>, V> reductionFunction;

        private Matching(@Nonnull final Function<Stream<V>, V> reductionFunction) {
            this.reductionFunction = reductionFunction;
        }

        @Nonnull
        @Override
        public V reduce(@Nonnull final Map<T, V> tertiaryEntries,
                        @Nonnull final T tertiaryKey,
                        @Nonnull final Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier,
                        @Nonnull final Function<V, V> valueWithEnvironmentDecorator,
                        @Nonnull final MetricCollector metricCollector) {
            // Note: the stored key is the receiver and the looked-up key is the argument. PhysicalPlanEquivalence
            // .equals is asymmetric, so swapping the two takes a different branch and changes which entries match.
            final var result = reductionFunction.apply(tertiaryEntries.entrySet().stream()
                    .filter(kvPair -> kvPair.getKey().equals(tertiaryKey))
                    .map(Map.Entry::getValue));
            if (result != null) {
                metricCollector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT);
                return valueWithEnvironmentDecorator.apply(result);
            }
            metricCollector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS);
            return supplyAndStore(tertiaryEntries, tertiaryKeyValueSupplier);
        }
    }

    /**
     * The write-only reducer. See {@link Reducer#writeOnly()}.
     *
     * @param <T> the type of the tertiary cache key
     * @param <V> the type of the value stored in the cache
     */
    static final class WriteOnly<T, V> extends Reducer<T, V> {

        private WriteOnly() {
        }

        @Nonnull
        @Override
        public V reduce(@Nonnull final Map<T, V> tertiaryEntries,
                        @Nonnull final T tertiaryKey,
                        @Nonnull final Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier,
                        @Nonnull final Function<V, V> valueWithEnvironmentDecorator,
                        @Nonnull final MetricCollector metricCollector) {
            final var value = supplyAndStore(tertiaryEntries, tertiaryKeyValueSupplier);
            metricCollector.increment(RelationalMetric.RelationalCount.PLAN_CACHE_WRITE_ONLY_STORE);
            return value;
        }
    }
}

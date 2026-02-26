/*
 * BoundRecordQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.expressions.Comparisons.ComparisonWithParameter;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.expressions.BooleanComponent.groupedComparisons;

/**
 * A bound record query wraps an instance of {@link RecordQuery} and {@link RecordStoreState} in order to associate
 * the query with the state of a store. That allows for differentiation of queries depending on the store state, e.g.
 * the availability of indexes at the time the query was bound to the state, etc.
 * <br>
 * Bound record queries also define {@link #hashCode()} and {@link #equals(Object)} in a way that allows for queries to
 * be considered compatible to each other which is useful with respect to pre-bound parameter markers.
 * <br>
 * Two queries are considered compatible, iff
 * 1. their query structure is equal (minus their pre-bound parameter markers)
 * 2. their associated record store state was compatible at the time of binding the {@link RecordQuery} to the
 *    {@link RecordStoreState}
 * 3. their pre-bound parameter markers use compatible pre-bound values
 */
public class BoundRecordQuery {
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nonnull
    private final RecordQuery recordQuery;
    @Nonnull
    private final Supplier<Set<String>> parametersSupplier = Suppliers.memoize(this::computeParameters);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final ParameterRelationshipGraph parameterRelationshipGraph;
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    public BoundRecordQuery(@Nonnull final RecordStoreState recordStoreState, @Nonnull final RecordQuery recordQuery) {
        this(recordStoreState, recordQuery, ParameterRelationshipGraph.empty());
    }

    public BoundRecordQuery(@Nonnull final RecordStoreState recordStoreState, @Nonnull final RecordQuery recordQuery, @Nonnull Bindings perBoundParameterBindings) {
        this(recordStoreState, recordQuery, ParameterRelationshipGraph.fromRecordQueryAndBindings(recordQuery, perBoundParameterBindings));
    }

    private BoundRecordQuery(@Nonnull final RecordStoreState recordStoreState, @Nonnull final RecordQuery recordQuery, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        this.recordStoreState = recordStoreState;
        this.recordQuery = recordQuery;
        this.parameterRelationshipGraph = parameterRelationshipGraph;
    }

    @Nonnull
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    @Nonnull
    public RecordQuery getRecordQuery() {
        return recordQuery;
    }

    public Set<String> getParameters() {
        return parametersSupplier.get();
    }

    @Nonnull
    public ParameterRelationshipGraph getParameterRelationshipGraph() {
        return parameterRelationshipGraph;
    }

    /**
     * Plan this query using the given store.
     * @param store the store to use
     * @return a record query plan for this query
     */
    @Nonnull
    public RecordQueryPlan plan(@Nonnull final FDBRecordStore store) {
        return store.planQuery(getRecordQuery(), parameterRelationshipGraph);
    }

    /**
     * Check if a given {@link RecordQueryPlan} for the given store and a set of bindings is compatible to this bound
     * query.
     * @param store record store to access
     * @param parameterBindings evaluation context containing parameter bindings
     * @return {@code true} if the plan, if it were to be executed on the given store and with the given parameter
     *         bindings, is compatible with this bound query; {@code false} otherwise
     */
    public boolean isCompatible(@Nonnull FDBRecordStore store,
                                @Nonnull Bindings parameterBindings) {
        if (!store.getRecordStoreState().compatibleWith(recordStoreState)) {
            return false;
        }

        return parameterRelationshipGraph.isCompatible(parameterBindings);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    @SuppressWarnings("UnstableApiUsage")
    private int computeHashCode() {
        final HashFunction hashFunction = Hashing.murmur3_32_fixed();
        final Hasher hasher = hashFunction.newHasher();
        hasher.putInt(recordQuery.hashCode());
        hasher.putInt(recordStoreState.getIndexStates().hashCode());
        final RecordMetaDataProto.DataStoreInfo storeHeader = recordStoreState.getStoreHeader();
        hasher.putInt(storeHeader.getMetaDataversion());
        hasher.putInt(storeHeader.getUserVersion());
        hasher.putInt(getParameters().hashCode());
        hasher.putInt(parameterRelationshipGraph.hashCode());
        return hasher.hash().asInt();
    }

    @Nonnull
    private Set<String> computeParameters() {
        final QueryComponent filter = recordQuery.getFilter();
        if (filter != null) {
            return groupedComparisons(filter)
                    .flatMap(pair -> pair.getRight().stream())
                    .map(ComparisonWithParameter::getParameter)
                    .collect(ImmutableSet.toImmutableSet());
        }
        return ImmutableSet.of();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BoundRecordQuery)) {
            return false;
        }
        final BoundRecordQuery other = (BoundRecordQuery)o;

        if (!getRecordQuery().equals(other.getRecordQuery())) {
            return false;
        }

        if (!getParameters().equals(other.getParameters())) {
            return false;
        }

        final RecordStoreState otherRecordStoreState = other.getRecordStoreState();
        final RecordMetaDataProto.DataStoreInfo storeHeader = recordStoreState.getStoreHeader();
        final RecordMetaDataProto.DataStoreInfo otherStoreHeader = otherRecordStoreState.getStoreHeader();
        if (storeHeader.getMetaDataversion() != otherStoreHeader.getMetaDataversion()) {
            return false;
        }
        if (storeHeader.getUserVersion() != otherStoreHeader.getUserVersion()) {
            return false;
        }
        if (!this.recordStoreState.compatibleWith(otherRecordStoreState)) {
            return false;
        }

        return parameterRelationshipGraph.equals(other.getParameterRelationshipGraph());
    }

    @Override
    public String toString() {
        return recordQuery.toString();
    }
}

/*
 * IndexPredicateExpansion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Trait that plugs index predicate expansion utility methods into the consumer.
 */
public class IndexPredicateExpansion {

    /**
     * Verifies that a given predicate is in a disjunctive normal form (DNF) and groups it into a mapping from a {@link Value}
     * and list of corresponding {@link RangeConstraints}.
     * <br>
     * For example: {@code OR(AND(v1, <3), AND(v2 >4), AND(v1<4))} will be transformed to the following:
     * {@code v1 -> [(-∞,3), (-∞, 4)], v2 -> [(4, +∞)]}.
     *
     * @param predicate The predicate to transform.
     * @return A mapping from a {@link Value} and list of corresponding {@link RangeConstraints}.
     */
    @Nonnull
    public static Optional<Multimap<Value, RangeConstraints>> dnfPredicateToRanges(@Nonnull final QueryPredicate predicate) {
        ImmutableMultimap.Builder<Value, RangeConstraints> result = ImmutableMultimap.builder();

        // simple case: x > 3 is DNF
        if (!(predicate instanceof OrPredicate)) {
            if (!conjunctionToRange(predicate, result, predicate)) {
                return Optional.empty();
            }
        } else {
            final var groups = predicate.getChildren();
            for (final var group : groups) {
                if (!conjunctionToRange(predicate, result, group)) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(result.build());
    }

    private static boolean conjunctionToRange(final @Nonnull QueryPredicate predicate, final ImmutableMultimap.Builder<Value, RangeConstraints> result, final QueryPredicate group) {
        if (group instanceof AndPredicate) {
            final var terms = ((AndPredicate)group).getChildren();
            Optional<Value> key = Optional.empty();
            final var rangeBuilder = RangeConstraints.newBuilder();
            for (final var term : terms) {
                if (!(term instanceof ValuePredicate)) {
                    return false;
                }
                final var valuePredicate = (ValuePredicate)term;
                if (key.isEmpty()) {
                    key = Optional.of(valuePredicate.getValue());
                } else {
                    if (!key.get().semanticEquals(valuePredicate.getValue(), AliasMap.emptyMap())) {
                        return false;
                    }
                }
                if (!rangeBuilder.addComparisonMaybe(valuePredicate.getComparison())) {
                    return false;
                }
            }
            final var range = rangeBuilder.build();
            if (key.isEmpty() || range.isEmpty()) {
                return false;
            }
            result.put(key.get(), range.get());
        } else {
            if (!(group instanceof ValuePredicate)) {
                return false;
            }
            final var valuePredicate = (ValuePredicate)group;
            final var key = valuePredicate.getValue();
            var rangeBuilder = RangeConstraints.newBuilder();
            if (!rangeBuilder.addComparisonMaybe(valuePredicate.getComparison())) {
                return false;
            }
            final var range = rangeBuilder.build();
            if (range.isEmpty()) {
                throw new RecordCoreException("invalid predicate").addLogInfo("predicate", predicate);
            }
            result.put(key, range.get());
        }
        return true;
    }
}

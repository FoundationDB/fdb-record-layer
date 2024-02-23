/*
 * MaxMatchMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Represents a max match between a (Rewritten) query result {@link Value} and the candidate result {@link Value}.
 */
public class MaxMatchMap {
    @Nonnull
    private final BiMap<Value, Value> mapping;
    @Nonnull
    private final Value queryResultValue; // in terms of the candidate quantifiers.
    @Nonnull
    private final Value candidateResultValue;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param mapping The {@link Value} mapping.
     * @param queryResult The query result from which the mapping keys originate.
     * @param candidateResult The candidate result from which the mapping values originate.
     */
    MaxMatchMap(@Nonnull final Map<Value, Value> mapping,
                @Nonnull final Value queryResult,
                @Nonnull final Value candidateResult) {
        this.mapping = ImmutableBiMap.copyOf(mapping);
        this.queryResultValue = queryResult;
        this.candidateResultValue = candidateResult;
    }

    @Nonnull
    public Value getCandidateResultValue() {
        return candidateResultValue;
    }

    @Nonnull
    public Value getQueryResultValue() {
        return queryResultValue;
    }

    @Nonnull
    public Map<Value, Value> getMapping() {
        return mapping;
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param rewrittenQueryValue the query {@code Value}, it must be translated using {@code this} translator.
     * @param candidateValue the candidate {@code Value} we want to search for maximum matches.
     *
     * @return A {@code Map} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculateMaxMatches(@Nonnull final AliasMap equivalenceAliasMap,
                                                  @Nonnull final Value rewrittenQueryValue,
                                                  @Nonnull final Value candidateValue) {
        final BiMap<Value, Value> newMapping = HashBiMap.create();
        //final var aliasMap = AliasMap.identitiesFor(candidateValue.getCorrelatedTo());
        rewrittenQueryValue.preOrderPruningIterator(queryValuePart -> {
            // now that we have rewritten this query value part using candidate value(s) we proceed to look it up in the candidate value.
            final var match = Streams.stream(candidateValue
                            // when traversing the candidate in pre-order, only descend into structures that can be referenced
                            // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                            // operator's children can not be referenced.
                            .preOrderPruningIterator(v -> v instanceof RecordConstructorValue || v instanceof FieldValue))
                    .filter(candidateValuePart -> queryValuePart.semanticEquals(candidateValuePart, equivalenceAliasMap))
                    .findAny();
            match.ifPresent(value -> newMapping.put(queryValuePart, value));
            return match.isEmpty();
        }).forEachRemaining(ignored -> {
        });
        return new MaxMatchMap(newMapping, rewrittenQueryValue, candidateValue);
    }
}

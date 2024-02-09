/*
 * Translator.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * This encapsulates a set of algorithms responsible for translating a given {@link Value} into another {@link Value} under
 * a set of given assumptions.
 * <br>
 * for example, suppose we have the following {@link RecordConstructorValue}s:
 * <ul>
 *     <li>{@code P = RCV(a, b)}</li>
 *     <li>{@code R = RCV(RCV(s, t), u, v)}</li>
 * </ul>
 * for a given {@code Value} such as {@code V = RCV(P.0, R.0.1)}, the translator can translate {@code V} in terms of <i>other</i>
 * {@code Value}s, e.g. if we also have something like:
 * <ul>
 *     <li>{@code P` = RCV(RCV(c, a), RCV(p, b))}</li>
 *     <li>{@code R` = RCV(w, x, RCV(RCV(t)))}</li>
 * </ul>
 * it can translate {@code V} into {@code RCV(P`.0.0, R`.2.0.0)}.
 * <br>
 * For certain shapes of {@code Value}s, multiple translations can be found, the {@code Translator} finds a translation
 * that matches the maximum part of the {@code Value} with sub-{@code Value} on the other side. For example:
 * <ul>
 *     <li>{@code R = RCV(s+t, s, t)}</li>
 * </ul>
 * for a given {@code Value} such as {@code V = RCV(R.0)}, the translator can translate {@code V} in terms of <i>other</i>
 * {@code Value}s, e.g. if we also have something like:
 * <ul>
 *     <li>{@code R` = RCV(s, t, (s+t))}</li>
 * </ul>
 * We could have the following translations for {@code V}:
 * <ul>
 *     <li>{@code RCV(R`.0 + R`.1}</li>
 *     <li>{@code RCV(R`.0.0}</li>
 * </ul>
 * The translator is guaranteed to choose the second instead of the first one, i.e. it finds the maximum match possible.
 * This property is very useful in the context of index matching where we want the query {@code Value} to be translated
 * by means of the index {@code Value}, the query {@code Value} should reuse as many sub-{@code Value}s as possible from
 * the index.
 */
public abstract class Translator {

    @Nonnull
    private final AliasMap aliasMap;

    public Translator(@Nonnull final AliasMap aliasMap) {
        this.aliasMap = aliasMap;
    }

    /**
     * Translates the {@link Value} {@code value} into another equivalent {@link Value} by means of the translation
     * rules.
     *
     * @param value The {@code Value} to translate.
     * @return an equivalent translated {@code Value}.
     */
    @Nonnull
    public abstract Value translate(@Nonnull final Value value);

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the {@code candidateValue}.
     * @param rewrittenQueryValue the query {@code Value}, it must be translated using {@code this} translator.
     * @param candidateValue the candidate {@code Value} we want to search for maximum matches.
     * @return A {@code Map} of all maximum matches.
     */
    @Nonnull
    public MaxMatchMap calculateMaxMatches(@Nonnull final Value rewrittenQueryValue,
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
                    .filter(candidateValuePart -> queryValuePart.semanticEquals(candidateValuePart, getAliasMap()))
                    .findAny();
            match.ifPresent(value -> newMapping.put(queryValuePart, value));
            return match.isEmpty();
        }).forEachRemaining(ignored -> {
        });
        return new MaxMatchMap(newMapping, rewrittenQueryValue, candidateValue);
    }

    @Nonnull
    public AliasMap getAliasMap() {
        return aliasMap;
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder of {@link Translator} objects.
     */
    public static class Builder {

        /**
         * Fluent builder of {@link Translator} objects.
         */
        public static class CompositeBuilder {

            @Nonnull
            private final ImmutableList.Builder<Translator> translatorsBuilder;

            public CompositeBuilder(@Nonnull Collection<Translator> translators) {
                this.translatorsBuilder = ImmutableList.builder();
                translatorsBuilder.addAll(translators);
            }

            @Nonnull
            public CompositeBuilder add(@Nonnull final Translator translator) {
                this.translatorsBuilder.add(translator);
                return this;
            }

            @Nonnull
            public CompositeBuilder addAll(@Nonnull final Collection<Translator> translators) {
                this.translatorsBuilder.addAll(translators);
                return this;
            }

            @Nonnull
            public CompositeTranslator build() {
                return new CompositeTranslator(translatorsBuilder.build());
            }
        }

        /**
         * Fluent builder of {@link Translator} objects.
         */
        public static class NonCompositeBuilder {

            protected CorrelationIdentifier queryCorrelation;

            protected  CorrelationIdentifier candidateCorrelation;

            protected AliasMap aliasMap;

            protected MaxMatchMap maxMatchMap;

            public NonCompositeBuilder(final CorrelationIdentifier queryCorrelation,
                                       final CorrelationIdentifier candidateCorrelation) {
                this.queryCorrelation = queryCorrelation;
                this.candidateCorrelation = candidateCorrelation;
                this.aliasMap = AliasMap.emptyMap();
            }

            @Nonnull
            public NonCompositeBuilder using(@Nonnull final MaxMatchMap maxMatchMap) {
                this.maxMatchMap = maxMatchMap;
                return this;
            }

            @Nonnull
            public NonCompositeBuilder withConstantAliasMap(@Nonnull final AliasMap aliasMap) {
                this.aliasMap = aliasMap;
                return this;
            }

            @Nonnull
            public NonCompositeBuilder ofCorrelations(@Nonnull final CorrelationIdentifier queryCorrelation,
                                                      @Nonnull final CorrelationIdentifier candidateCorrelation) {
                this.queryCorrelation = queryCorrelation;
                this.candidateCorrelation = candidateCorrelation;
                return this;
            }

            @Nonnull
            public Translator build() {
                if (maxMatchMap == null) {
                    return new SimpleTranslator(queryCorrelation, candidateCorrelation, aliasMap);
                } else {
                    return new MaxMatchMapTranslator(maxMatchMap, queryCorrelation, candidateCorrelation, aliasMap);
                }
            }
        }

        @Nonnull
        public NonCompositeBuilder ofCorrelations(@Nonnull final CorrelationIdentifier queryCorrelation,
                                                  @Nonnull final CorrelationIdentifier candidateCorrelation) {
            return new NonCompositeBuilder(queryCorrelation, candidateCorrelation);
        }

        @Nonnull
        public CompositeBuilder compose(@Nonnull final Collection<Translator> translators) {
            return new CompositeBuilder(translators);
        }
    }
}

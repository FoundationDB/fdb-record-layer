/*
 * ValueComparisonRangePredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.KeyExpressionUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueRangesPredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;

    protected ValueRangesPredicate(@Nonnull final Value value) {
        this.value = value;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!PredicateWithValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ValueRangesPredicate that = (ValueRangesPredicate)other;
        return value.semanticEquals(that.value, equivalenceMap);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.planHash(hashKind, value);
    }

    public static Placeholder placeholder(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, parameterAlias);
    }

    public static Sargable sargable(@Nonnull Value value, @Nonnull final CompileTimeRange range) {
        return new Sargable(value, range);
    }

    /**
     * A placeholder predicate solely used for index matching.
     */
    @SuppressWarnings("java:S2160")
    public static class Placeholder extends ValueRangesPredicate {

        @Nonnull
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Place-Holder");

        @Nonnull
        private final CorrelationIdentifier alias;

        @Nullable
        private final CompileTimeRange compileTimeRange;

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier alias) {
            this(value, alias, null);
        }

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier alias, @Nullable CompileTimeRange compileTimeRange) {
            super(value);
            this.alias = alias;
            this.compileTimeRange = compileTimeRange;
        }

        @Nonnull
        @Override
        public Placeholder withValue(@Nonnull final Value value) {
            return new Placeholder(value, alias);
        }

        @Nonnull
        public Placeholder withCompileTimeRange(@Nonnull final CompileTimeRange range) {
            return new Placeholder(getValue(), alias, range);
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nullable
        public CompileTimeRange getComparisons() {
            return compileTimeRange;
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("this method should not ever be reached");
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            return Objects.equals(alias, ((Placeholder)other).alias);
        }

        public boolean semanticEqualsWithoutParameterAlias(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return super.semanticEquals(other, aliasMap);
        }

        @Nonnull
        @Override
        public Placeholder translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
            return new Placeholder(getValue().translateCorrelations(translationMap), alias);
        }

        @Nonnull
        @Override
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap, @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
            throw new RecordCoreException("this method should not ever be reached");
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), alias);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
        }

        @Override
        public String toString() {
            return "(" + getValue() + " -> " + alias + ")";
        }
    }

    /**
     * A query predicate that can be used as a (s)earch (arg)ument for an index scan.
     */
    @SuppressWarnings("java:S2160")
    public static class Sargable extends ValueRangesPredicate implements QueryPredicate.Serializable {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sargable-Predicate");

        @Nonnull
        private final CompileTimeRange range;

        public Sargable(@Nonnull final Value value, @Nonnull final CompileTimeRange range) {
            super(value);
            this.range = range;
        }

        @Nonnull
        @Override
        public Sargable withValue(@Nonnull final Value value) {
            return new Sargable(value, range);
        }

        @Nonnull
        public CompileTimeRange getRange() {
            return range;
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
            return ImmutableSet.<CorrelationIdentifier>builder()
                    .addAll(super.getCorrelatedToWithoutChildren())
                    .addAll(range.getCorrelatedTo())
                    .build();
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("search arguments should never be evaluated");
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
            if (this == other) {
                return true;
            }

            if (!super.equalsWithoutChildren(other, equivalenceMap)) {
                return false;
            }

            if (!(other instanceof Sargable)) {
                return false;
            }
            final var that = (Sargable)other;
            return range.semanticEquals(that.range, equivalenceMap);
        }

        @Nonnull
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Sargable translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
            final var translatedValue = getValue().translateCorrelations(translationMap);
            final var newRange = range.rebase(translationMap.getAliasMapMaybe().orElseThrow(() -> new RecordCoreException("must have aliasMap")));
            if (translatedValue != getValue() || newRange != range) {
                return new Sargable(translatedValue, newRange);
            }
            return this;
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), range.semanticHashCode());
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, range);
        }

        @Nonnull
        @Override
        public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap,
                                                                    @Nonnull final QueryPredicate candidatePredicate) {
            if (candidatePredicate instanceof Placeholder) {
                final Placeholder candidatePlaceholder = (Placeholder)candidatePredicate;

                // the value on which the placeholder is defined must be the same as the sargable's.
                if (!getValue().semanticEquals(candidatePlaceholder.getValue(), aliasMap)) {
                    return Optional.empty();
                }

                // if the placeholder has a compile-time range (filtered index) check to see whether it implies
                // (some) of the predicate comparisons.
                if (candidatePlaceholder.compileTimeRange != null) {
                    if (!range.isEmpty() && candidatePlaceholder.compileTimeRange.implies(range)) {
                        return Optional.of(new PredicateMapping(this,
                                candidatePredicate,
                                ((partialMatch, boundParameterPrefixMap) -> {
                                    if (boundParameterPrefixMap.containsKey(candidatePlaceholder.getAlias())) {
                                        return Optional.empty();
                                    }
                                    return injectCompensationFunctionMaybe();
                                }),
                                candidatePlaceholder.getAlias()));
                    } else {
                        return Optional.empty();
                    }

                } else {
                    // we found a compatible association between a comparison range in the query and a
                    // parameter placeholder in the candidate
                    return Optional.of(new PredicateMapping(this,
                            candidatePredicate,
                            ((partialMatch, boundParameterPrefixMap) -> {
                                if (boundParameterPrefixMap.containsKey(candidatePlaceholder.getAlias())) {
                                    if (boundParameterPrefixMap.containsKey(candidatePlaceholder.getAlias())) {
                                        return Optional.empty();
                                    }
                                    return injectCompensationFunctionMaybe();
                                }
                                return injectCompensationFunctionMaybe();
                            }),
                            candidatePlaceholder.getAlias()));
                }
            } else if (candidatePredicate.isTautology()) {
                return Optional.of(new PredicateMapping(this,
                        candidatePredicate,
                        ((partialMatch, boundParameterPrefixMap) -> injectCompensationFunctionMaybe())));
            }

            //
            // The candidate predicate is not a placeholder which means that the match candidate can not be
            // parameterized by a mapping of this to the candidate predicate. Therefore, in order to match at all,
            // it must be semantically equivalent.
            //
            if (semanticEquals(candidatePredicate, aliasMap)) {
                // Note that we never have to reapply the predicate as both sides are always semantically
                // equivalent.
                return Optional.of(new PredicateMapping(this,
                        candidatePredicate,
                        ((matchInfo, boundParameterPrefixMap) -> Optional.empty())));
            }

            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                    @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                    @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
            Verify.verify(childrenResults.isEmpty());
            return injectCompensationFunctionMaybe();
        }

        @Nonnull
        public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe() {
            return Optional.of(reapplyPredicate());
        }

        private ExpandCompensationFunction reapplyPredicate() {
            return translationMap -> GraphExpansion.ofPredicate(toResidualPredicate().translateCorrelations(translationMap));
        }

        /**
         * transforms this Sargable into a conjunction of equality and non-equality predicates.
         * @return a conjunction of equality and non-equality predicates.
         */
        @Override
        @Nonnull
        public QueryPredicate toResidualPredicate() {
            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            residuals.addAll(range.getComparisons().stream().map(c -> getValue().withComparison(c)).collect(Collectors.toList()));
            return AndPredicate.and(residuals.build());
        }

        @Override
        public String toString() {
            return getValue() + " " + range;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            final var protoBuilder = RecordMetaDataProto.Sargable.newBuilder();
            protoBuilder.setValue(KeyExpressionUtils.toKeyExpression(getValue()).toKeyExpression());
            protoBuilder.addAllComparisons(range.getComparisons().stream().map(comparison -> ((Comparisons.Comparison.Serializable)comparison).toProto()).collect(Collectors.toList()));
            return RecordMetaDataProto.Predicate.newBuilder().setSargable(protoBuilder.build()).build();
        }

        @Override
        public boolean isSerializable() {
            return KeyExpressionUtils.convertibleToKeyExpression(getValue()) &&
                   range.getComparisons().stream().allMatch(Comparisons.Comparison::isSerializable);
        }

        @Nonnull
        public static Sargable deserialize(@Nonnull final RecordMetaDataProto.Sargable proto,
                                           @Nonnull final CorrelationIdentifier alias,
                                           @Nonnull final Type inputType) {
            Verify.verify(proto.hasValue(), String.format("attempt to deserialize %s without value", Sargable.class));
            final var value = new ScalarTranslationVisitor(KeyExpression.fromProto(proto.getValue())).toResultValue(alias, inputType);
            final var comparisons = proto.getComparisonsList().stream().map(Comparisons.Comparison::deserialize).collect(Collectors.toList());
            final var compileTimeRangeBuilder = CompileTimeRange.newBuilder();
            for (final var comparison : comparisons) {
                Verify.verify(compileTimeRangeBuilder.tryAdd(comparison), String.format("attempt to add non-compile-time range '%s'", comparison));
            }
            return new Sargable(value, compileTimeRangeBuilder.build().orElseThrow(() -> new RecordCoreException(String.format("could not create compile-time range from '%s'", comparisons))));
        }
    }
}
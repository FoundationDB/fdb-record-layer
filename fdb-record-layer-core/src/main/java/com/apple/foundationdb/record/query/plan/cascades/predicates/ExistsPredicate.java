/*
 * ExistsPredicate.java
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PExistsPredicate;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
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

/**
 * An existential predicate that is true if the inner correlation produces any values, and false otherwise.
 */
@API(API.Status.EXPERIMENTAL)
public class ExistsPredicate extends AbstractQueryPredicate implements LeafQueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Predicate");

    @Nonnull
    private final CorrelationIdentifier existentialAlias;

    private ExistsPredicate(@Nonnull final PlanSerializationContext serializationContext,
                            @Nonnull final PExistsPredicate existsPredicateProto) {
        super(serializationContext, Objects.requireNonNull(existsPredicateProto.getSuper()));
        this.existentialAlias = CorrelationIdentifier.of(Objects.requireNonNull(existsPredicateProto.getExistentialAlias()));
    }

    public ExistsPredicate(@Nonnull final CorrelationIdentifier existentialAlias) {
        super(false);
        this.existentialAlias = existentialAlias;
    }

    @Nonnull
    public CorrelationIdentifier getExistentialAlias() {
        return existentialAlias;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new RecordCoreException("this predicate cannot be evaluated per record");
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        final var resultBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        resultBuilder.add(existentialAlias);
        return resultBuilder.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ExistsPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        final var quantifiedObjectValue =
                QuantifiedObjectValue.of(existentialAlias, Type.any());

        //
        // Note that the following cast must work! If it does not we are in a bad spot and we should fail.
        //
        final var translatedQuantifiedObjectValue =
                (QuantifiedObjectValue)quantifiedObjectValue
                        .translateCorrelations(translationMap);
        if (quantifiedObjectValue == translatedQuantifiedObjectValue) {
            return this;
        }
        return new ExistsPredicate(translatedQuantifiedObjectValue.getAlias());
    }

    @Nonnull
    @Override
    public QueryPredicate toResidualPredicate() {
        return new ValuePredicate(QuantifiedObjectValue.of(existentialAlias, new Type.Any()),
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                                       @Nonnull final ValueEquivalence valueEquivalence) {
        return LeafQueryPredicate.super.equalsWithoutChildren(other, valueEquivalence)
                .compose(ignored ->  {
                    final ExistsPredicate that = (ExistsPredicate)other;
                    if (existentialAlias.equals(that.existentialAlias)) {
                        return BooleanWithConstraint.alwaysTrue();
                    }
                    return valueEquivalence.isDefinedEqual(existentialAlias, that.existentialAlias);
                });
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return LeafQueryPredicate.super.computeSemanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final ValueEquivalence valueEquivalence,
                                                                @Nonnull final QueryPredicate candidatePredicate,
                                                                @Nonnull final EvaluationContext evaluationContext) {
        if (candidatePredicate instanceof Placeholder) {
            return Optional.empty();
        } else if (candidatePredicate instanceof ExistsPredicate) {
            final ExistsPredicate candidateExistsPredicate = (ExistsPredicate)candidatePredicate;
            final var aliasEquals =
                    valueEquivalence.isDefinedEqual(existentialAlias, candidateExistsPredicate.getExistentialAlias());
            if (aliasEquals.isFalse()) {
                return Optional.empty();
            }
            return Optional.of(PredicateMapping.regularMapping(this, candidatePredicate,
                    this::injectCompensationFunctionMaybe, Optional.empty(),
                    aliasEquals.getConstraint(), Optional.empty()));  // TODO: provide a translated predicate value here.
        } else if (candidatePredicate.isTautology()) {
            return Optional.of(PredicateMapping.regularMapping(this, candidatePredicate,
                    this::injectCompensationFunctionMaybe, Optional.empty()));  // TODO: provide a translated predicate value here.
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                @Nonnull final List<Optional<ExpandCompensationFunction>> childrenResults) {
        Verify.verify(childrenResults.isEmpty());
        return injectCompensationFunctionMaybe(partialMatch, boundParameterPrefixMap);
    }

    @Nonnull
    public Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var childPartialMatchOptional = matchInfo.getChildPartialMatch(existentialAlias);
        final var compensationOptional =
                childPartialMatchOptional.map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap));
        if (compensationOptional.isEmpty() || compensationOptional.get().isNeededForFiltering()) {
            return Optional.of(translationMap -> injectCompensation(partialMatch, translationMap));
        }
        return Optional.empty();
    }

    @Nonnull
    private Set<QueryPredicate> injectCompensation(@Nonnull final PartialMatch partialMatch, @Nonnull final TranslationMap translationMap) {
        Verify.verify(!translationMap.containsSourceAlias(existentialAlias));

        final var containingExpression = partialMatch.getQueryExpression();
        Verify.verify(containingExpression.canCorrelate());

        return LinkedIdentitySet.of(this);
    }
    
    @Override
    public String toString() {
        return "∃" + existentialAlias;
    }

    @Nonnull
    @Override
    public PExistsPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PExistsPredicate.newBuilder()
                .setSuper(toAbstractQueryPredicateProto(serializationContext))
                .setExistentialAlias(existentialAlias.getId()).build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setExistsPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ExistsPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PExistsPredicate existsPredicateProto) {
        return new ExistsPredicate(serializationContext, existsPredicateProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PExistsPredicate, ExistsPredicate> {
        @Nonnull
        @Override
        public Class<PExistsPredicate> getProtoMessageClass() {
            return PExistsPredicate.class;
        }

        @Nonnull
        @Override
        public ExistsPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PExistsPredicate existsPredicateProto) {
            return ExistsPredicate.fromProto(serializationContext, existsPredicateProto);
        }
    }
}

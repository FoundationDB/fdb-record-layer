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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
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
    public ExistsPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        if (translationMap.containsSourceAlias(existentialAlias)) {
            return new ExistsPredicate(translationMap.getTargetAliasOrDefault(existentialAlias, existentialAlias));
        } else {
            return this;
        }
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
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap aliasMap) {
        if (!LeafQueryPredicate.super.equalsWithoutChildren(other, aliasMap)) {
            return false;
        }
        final ExistsPredicate that = (ExistsPredicate)other;
        return aliasMap.containsMapping(existentialAlias, that.existentialAlias);
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
        return planHash();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap, @Nonnull final QueryPredicate candidatePredicate, final @Nonnull EvaluationContext evaluationContext) {
        if (candidatePredicate instanceof Placeholder) {
            return Optional.empty();
        } else if (candidatePredicate instanceof ExistsPredicate) {
            final ExistsPredicate candidateExistsPredicate = (ExistsPredicate)candidatePredicate;
            if (!existentialAlias.equals(aliasMap.getTarget(candidateExistsPredicate.getExistentialAlias()))) {
                return Optional.empty();
            }
            return Optional.of(PredicateMapping.regularMapping(this, candidatePredicate, this::injectCompensationFunctionMaybe));
        } else if (candidatePredicate.isTautology()) {
            return Optional.of(PredicateMapping.regularMapping(this, candidatePredicate, this::injectCompensationFunctionMaybe));
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
        final var compensationOptional = childPartialMatchOptional.map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap));
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
}

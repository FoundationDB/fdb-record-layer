/*
 * ExistsPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PredicateMultiMap.PredicateMapping;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An existential predicate that is true if the inner correlation produces any values, and false otherwise.
 */
@API(API.Status.EXPERIMENTAL)
public class ExistsPredicate implements LeafQueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Predicate");
    @Nonnull
    private final CorrelationIdentifier existentialAlias;

    @Nonnull
    private final QueryComponent alternativeComponent;

    public ExistsPredicate(@Nonnull final CorrelationIdentifier existentialAlias, @Nonnull final QueryComponent alternativeComponent) {
        this.existentialAlias = existentialAlias;
        this.alternativeComponent = alternativeComponent;
    }

    @Nonnull
    public CorrelationIdentifier getExistentialAlias() {
        return existentialAlias;
    }


    @Nonnull
    public QueryComponent getAlternativeComponent() {
        return alternativeComponent;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        throw new RecordCoreException("this predicate cannot be evaluated per record");
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(existentialAlias);
    }

    @Nonnull
    @Override
    public ExistsPredicate rebaseLeaf(@Nonnull final AliasMap translationMap) {
        if (translationMap.containsSource(existentialAlias)) {
            return new ExistsPredicate(translationMap.getTargetOrThrow(existentialAlias), alternativeComponent);
        } else {
            return this;
        }
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!LeafQueryPredicate.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ExistsPredicate that = (ExistsPredicate)other;
        return equivalenceMap.containsMapping(existentialAlias, that.existentialAlias);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
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
    public Optional<PredicateMapping> impliesCandidatePredicate(@NonNull final AliasMap aliasMap, @Nonnull final QueryPredicate candidatePredicate) {
        if (candidatePredicate instanceof ExistsPredicate) {
            final ExistsPredicate candidateExistsPredicate = (ExistsPredicate)candidatePredicate;
            if (!existentialAlias.equals(aliasMap.getTarget(candidateExistsPredicate.getExistentialAlias()))) {
                return Optional.empty();
            }
            return Optional.of(new PredicateMapping(this, candidatePredicate, this::reapplyPredicateMaybe));
        } else if (candidatePredicate.isTautology()) {
            return Optional.of(new PredicateMapping(this, candidatePredicate, this::reapplyPredicateMaybe));
        }
        return Optional.empty();
    }

    @Nonnull
    public Optional<QueryPredicate> reapplyPredicateMaybe(@Nonnull final MatchInfo matchInfo, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final Optional<PartialMatch> childPartialMatchOptional = matchInfo.getChildPartialMatch(existentialAlias);
        final Optional<Compensation> compensationOptional = childPartialMatchOptional.map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap));
        if (!compensationOptional.isPresent() || compensationOptional.get().isNeededForFiltering()) {
            // TODO we are presently unable to do much better than a reapplication of the alternative QueryComponent
            //      make a predicate that can evaluate a QueryComponent
            return Optional.of(toResidualPredicate());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public QueryPredicate toResidualPredicate() {
        return new QueryComponentPredicate(getAlternativeComponent(), existentialAlias);
    }

    @Override
    public String toString() {
        return "∃" + existentialAlias;
    }
}

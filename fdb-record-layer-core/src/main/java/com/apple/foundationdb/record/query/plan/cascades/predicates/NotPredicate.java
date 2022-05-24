/*
 * NotPredicate.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link QueryPredicate} that is satisfied when its child component is not satisfied.
 *
 * For tri-valued logic, if the child evaluates to unknown / {@code null}, {@code NOT} is still unknown.
 */
@API(API.Status.EXPERIMENTAL)
public class NotPredicate implements QueryPredicateWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Predicate");

    @Nonnull
    public final QueryPredicate child;

    public NotPredicate(@Nonnull QueryPredicate child) {
        this.child = child;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        return invert(child.eval(store, context));
    }

    @Nullable
    private Boolean invert(@Nullable Boolean v) {
        if (v == null) {
            return null;
        } else {
            return !v;
        }
    }

    @Nonnull
    @Override
    public QueryPredicate getChild() {
        return child;
    }

    @Override
    public String toString() {
        return "Not(" + getChild() + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(getChild());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getChild().planHash(hashKind) + 1;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public NotPredicate withChild(@Nonnull final QueryPredicate newChild) {
        return new NotPredicate(newChild);
    }

    @Nonnull
    @Override
    public Optional<PredicateMultiMap.ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                                  @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                                  @Nonnull final List<Optional<PredicateMultiMap.ExpandCompensationFunction>> childrenResults) {
        Verify.verify(childrenResults.size() == 1);
        final var childInjectCompensationFunctionOptional = Iterables.getOnlyElement(childrenResults);
        if (childInjectCompensationFunctionOptional.isEmpty()) {
            return Optional.empty();
        }
        final var childInjectCompensationFunction = childInjectCompensationFunctionOptional.get();

        return Optional.of(translationMap -> {
            final var childGraphExpansion = childInjectCompensationFunction.applyCompensation(translationMap);
            Verify.verify(childGraphExpansion.getResultColumns().isEmpty());
            return GraphExpansion.of(ImmutableList.of(),
                    ImmutableList.of(not(childGraphExpansion.asAndPredicate())),
                    childGraphExpansion.getQuantifiers(),
                    ImmutableList.of());
        });
    }

    @Nonnull
    public static NotPredicate not(@Nonnull final QueryPredicate predicate) {
        return new NotPredicate(predicate);
    }
}

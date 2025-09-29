/*
 * RelationalExpressionWithChildren.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A parent interface for {@link RelationalExpression}s that have relational children (as opposed to non-relation
 * children, such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}s).
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalExpressionWithChildren extends RelationalExpression {
    int getRelationalChildCount();

    @Nonnull
    @SuppressWarnings("squid:S2201")
    default Set<CorrelationIdentifier> computeCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        final List<? extends Quantifier> quantifiers = getQuantifiers();
        final Map<CorrelationIdentifier, ? extends Quantifier> aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(quantifiers);

        getCorrelatedToWithoutChildren()
                .stream()
                .filter(correlationIdentifier -> !aliasToQuantifierMap.containsKey(correlationIdentifier))
                .forEach(builder::add);

        for (final Quantifier quantifier : quantifiers) {
            quantifier.getCorrelatedTo()
                    .stream()
                    // Filter out the correlations that are satisfied by this expression if this expression can
                    // correlate.
                    .filter(correlationIdentifier -> !canCorrelate() || !aliasToQuantifierMap.containsKey(correlationIdentifier))
                    .forEach(builder::add);
        }

        return builder.build();
    }

    @Nonnull
    Set<CorrelationIdentifier> getCorrelatedToWithoutChildren();

    @Nonnull
    @Override
    default Set<Quantifier> getMatchedQuantifiers(@Nonnull final PartialMatch partialMatch) {
        return partialMatch.getMatchedQuantifiers();
    }

    @Nonnull
    @Override
    default PartiallyOrderedSet<CorrelationIdentifier> getCorrelationOrder() {
        if (canCorrelate()) {
            final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(getQuantifiers());
            return PartiallyOrderedSet.of(
                    getQuantifiers().stream()
                            .map(Quantifier::getAlias)
                            .collect(ImmutableSet.toImmutableSet()),
                    alias -> Objects.requireNonNull(aliasToQuantifierMap.get(alias)).getCorrelatedTo());
        } else {
            return PartiallyOrderedSet.empty();
        }
    }

    /**
     * Tag interface to signal that the children this expression owns, that is the owned {@link Quantifier}s, are
     * to be treated as an unordered set. This is true for implementors like {@link SelectExpression} or
     * {@link LogicalUnionExpression}, but not
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan}.
     * This interface allows the respective classes to announce their behavior which may impact matching performance
     * drastically.
     */
    interface ChildrenAsSet extends RelationalExpressionWithChildren {
        // nothing
    }
}

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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.TopologicalSort;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A parent interface for {@link RelationalExpression}s that have relational children (as opposed to non-relation
 * children, such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}s).
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalExpressionWithChildren extends RelationalExpression {
    int getRelationalChildCount();

    @Nonnull
    @Override
    @SuppressWarnings("squid:S2201")
    default Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        final List<? extends Quantifier> quantifiers = getQuantifiers();

        final Map<CorrelationIdentifier, ? extends Quantifier> aliasToQuantifierMap = quantifiers.stream()
                        .collect(Collectors.toMap(Quantifier::getAlias, Function.identity()));

        // We should check if the graph is sound here, if it is not we should throw an exception. This method
        // will properly return with an empty. There are other algorithms that may not be as defensive and we
        // must protect ourselves from illegal graphs (and bugs).
        final Optional<List<CorrelationIdentifier>> orderedOptional =
                TopologicalSort.anyTopologicalOrderPermutation(
                        quantifiers.stream()
                                .map(Quantifier::getAlias)
                                .collect(Collectors.toSet()),
                        alias -> Objects.requireNonNull(aliasToQuantifierMap.get(alias)).getCorrelatedTo());

        orderedOptional.orElseThrow(() -> new IllegalArgumentException("correlations are cyclic"));

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
    default RelationalExpressionWithChildren rebase(@Nonnull final AliasMap translationMap) {
        final List<Quantifier> rebasedQuantifiers = getQuantifiers().stream()
                .map(quantifier -> quantifier.rebase(translationMap))
                .collect(Collectors.toList());
        return rebaseWithRebasedQuantifiers(translationMap,
                rebasedQuantifiers);
    }

    @Nonnull
    RelationalExpressionWithChildren rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                  @Nonnull final List<Quantifier> rebasedQuantifiers);

    @Nonnull
    @Override
    default Set<Quantifier.ForEach> computeUnmatchedForEachQuantifiers(@Nonnull final PartialMatch partialMatch) {
        final MatchInfo matchInfo = partialMatch.getMatchInfo();
        final Set<Quantifier.ForEach> unmappedForEachQuantifiers = new LinkedIdentitySet<>();
        for (final Quantifier quantifier : getQuantifiers()) {
            if (quantifier instanceof Quantifier.ForEach && !matchInfo.getChildPartialMatch(quantifier.getAlias()).isPresent()) {
                unmappedForEachQuantifiers.add((Quantifier.ForEach)quantifier);
            }
        }
        return unmappedForEachQuantifiers;
    }
}

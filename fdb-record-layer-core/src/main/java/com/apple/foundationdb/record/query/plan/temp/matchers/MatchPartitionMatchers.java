/*
 * MatchPartitionMatchers.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for {@link MatchPartition}.
 */
public class MatchPartitionMatchers {
    private MatchPartitionMatchers() {
        // do not instantiate
    }

    @Nonnull
    public static <C extends Collection<? extends PartialMatch>> BindingMatcher<MatchPartition> ofMatches(@Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(MatchPartition.class,
                Extractor.of(MatchPartition::getPartialMatches, name -> "partialMatches(" + name + ")"),
                downstream);
    }

    public static <R extends RelationalExpression, C extends Collection<? extends PartialMatch>> BindingMatcher<MatchPartition> ofExpressionAndMatches(@Nonnull final BindingMatcher<R> downstreamExpression,
                                                                                                                                                       @Nonnull final BindingMatcher<C> downstreamMatches) {
        return ofExpressionOptionalAndMatches(OptionalIfPresentMatcher.present(downstreamExpression),
                downstreamMatches);
    }

    public static <O extends Optional<RelationalExpression>, C extends Collection<? extends PartialMatch>> BindingMatcher<MatchPartition> ofExpressionOptionalAndMatches(@Nonnull final BindingMatcher<O> downstreamExpressionOptional,
                                                                                                                                                                         @Nonnull final BindingMatcher<C> downstreamMatches) {
        return typedWithDownstream(MatchPartition.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(MatchPartition.class,
                        ImmutableList.of(
                                typedWithDownstream(MatchPartition.class,
                                        Extractor.of(matchPartition -> {
                                            final Set<PartialMatch> partialMatches = matchPartition.getPartialMatches();
                                            final Iterator<PartialMatch> iterator = partialMatches.iterator();
                                            RelationalExpression lastExpression = null;
                                            while (iterator.hasNext()) {
                                                final PartialMatch partialMatch = iterator.next();
                                                if (lastExpression == null) {
                                                    lastExpression = partialMatch.getQueryExpression();
                                                } else if (lastExpression != partialMatch.getQueryExpression()) {
                                                    return Optional.empty();
                                                }
                                            }
                                            return Optional.ofNullable(lastExpression);
                                        }, name -> "expressionOptional(" + name + ")"),
                                        downstreamExpressionOptional),
                                typedWithDownstream(MatchPartition.class,
                                        Extractor.of(MatchPartition::getPartialMatches, name -> "partialMatches(" + name + ")"),
                                        downstreamMatches))));
    }
}

/*
 * References.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;

/**
 * Utility methods for {@link Reference}s.
 */
public class References {
    private References() {
        // do not instantiate
    }

    public static List<? extends Reference> translateGraphs(@Nonnull final List<? extends Reference> refs,
                                                            @Nonnull final Memoizer memoizer,
                                                            @Nonnull final TranslationMap translationMap,
                                                            final boolean shouldSimplifyValues) {
        if (refs.isEmpty()) {
            return ImmutableList.of();
        }

        final var partialOrder = referencesAndDependencies().evaluate(refs);

        final var references =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles"));

        final var translationsCache =
                Maps.<Reference, Reference>newIdentityHashMap();

        for (final var reference : references) {
            if (reference.getCorrelatedTo().stream().anyMatch(translationMap::containsSourceAlias)) {
                rebaseGraph(memoizer, translationMap, shouldSimplifyValues, reference, translationsCache,
                        alias -> {
                            Verify.verify(!translationMap.containsSourceAlias(alias), "illegal translate");
                            return alias;
                        });
            } else {
                translationsCache.put(reference, reference);
            }
        }

        return refs.stream()
                .map(ref -> Objects.requireNonNull(translationsCache.get(ref)))
                .collect(ImmutableList.toImmutableList());
    }

    public static List<? extends Reference> rebaseGraphs(@Nonnull final List<? extends Reference> refs,
                                                         @Nonnull final Memoizer memoizer,
                                                         @Nonnull final TranslationMap translationMap,
                                                         final boolean shouldSimplifyValues) {
        if (refs.isEmpty()) {
            return ImmutableList.of();
        }

        final var partialOrder = referencesAndDependencies().evaluate(refs);

        final var references =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles"));

        final var translationsCache =
                Maps.<Reference, Reference>newIdentityHashMap();

        for (final var reference : references) {
            rebaseGraph(memoizer, translationMap, shouldSimplifyValues, reference, translationsCache,
                    translationMap::getTarget);
        }

        return refs.stream()
                .map(ref -> Objects.requireNonNull(translationsCache.get(ref)))
                .collect(ImmutableList.toImmutableList());
    }

    private static void rebaseGraph(@Nonnull final Memoizer memoizer, @Nonnull final TranslationMap translationMap,
                                    final boolean shouldSimplifyValues, @Nonnull final Reference reference,
                                    @Nonnull final IdentityHashMap<Reference, Reference> translationsCache,
                                    @Nonnull final Function<CorrelationIdentifier, CorrelationIdentifier> quantifierAliasMapper) {
        var allMembersSame = true;
        final var translatedExploratoryExpressionsBuilder = ImmutableList.<RelationalExpression>builder();
        final var translatedFinalExpressionsBuilder = ImmutableList.<RelationalExpression>builder();
        for (final var expression : reference.getAllMemberExpressions()) {
            var allChildTranslationsSame = true;
            final var translatedQuantifiersBuilder = ImmutableList.<Quantifier>builder();
            for (final var quantifier : expression.getQuantifiers()) {
                final var childReference = quantifier.getRangesOver();

                // these must exist
                Verify.verify(translationsCache.containsKey(childReference));
                final var translatedChildReference = translationsCache.get(childReference);
                final var alias = quantifier.getAlias();
                if (translatedChildReference == childReference &&
                        !translationMap.containsSourceAlias(alias)) {
                    translatedQuantifiersBuilder.add(quantifier);
                } else {
                    translatedQuantifiersBuilder.add(
                            quantifier.overNewReference(translatedChildReference,
                                    quantifierAliasMapper.apply(alias)));
                    allChildTranslationsSame = false;
                }
            }

            final var translatedQuantifiers = translatedQuantifiersBuilder.build();
            final RelationalExpression translatedExpression;

            // we may not have to translate the current member
            if (allChildTranslationsSame) {
                final Set<CorrelationIdentifier> memberCorrelatedTo;
                if (expression instanceof RelationalExpressionWithChildren) {
                    memberCorrelatedTo = ((RelationalExpressionWithChildren)expression).getCorrelatedToWithoutChildren();
                } else {
                    memberCorrelatedTo = expression.getCorrelatedTo();
                }

                if (memberCorrelatedTo.stream().noneMatch(translationMap::containsSourceAlias)) {
                    translatedExpression = expression;
                } else {
                    translatedExpression = expression.translateCorrelations(translationMap, shouldSimplifyValues,
                            translatedQuantifiers);
                    Debugger.withDebugger(debugger -> debugger.onEvent(
                            new Debugger.TranslateCorrelationsEvent(translatedExpression, Debugger.Location.COUNT)));
                    allMembersSame = false;
                }
            } else {
                translatedExpression = expression.translateCorrelations(translationMap, shouldSimplifyValues,
                        translatedQuantifiers);
                Debugger.withDebugger(debugger -> debugger.onEvent(
                        new Debugger.TranslateCorrelationsEvent(translatedExpression, Debugger.Location.COUNT)));
                allMembersSame = false;
            }
            if (reference.isFinal(expression)) {
                translatedFinalExpressionsBuilder.add(translatedExpression);
            }
            if (reference.isExploratory(expression)) {
                translatedExploratoryExpressionsBuilder.add(translatedExpression);
            }
        }
        if (allMembersSame) {
            translationsCache.put(reference, reference);
        } else {
            final var translatedExploratoryExpressions =
                    translatedExploratoryExpressionsBuilder.build();
            final var translatedFinalExpressions =
                    translatedFinalExpressionsBuilder.build();

            final Reference translatedReference;
            if (!translatedExploratoryExpressions.isEmpty()) {
                translatedReference = memoizer.memoizeExploratoryExpressions(translatedExploratoryExpressions);
            } else {
                translatedReference = memoizer.memoizeFinalExpressions(translatedFinalExpressions);
            }

            translationsCache.put(reference, translatedReference);
        }
    }
}

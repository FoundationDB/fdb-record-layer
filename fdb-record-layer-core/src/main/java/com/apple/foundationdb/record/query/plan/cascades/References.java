/*
 * References.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.TranslateCorrelationsPlannerEvent;
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

    /**
     * Method to translate correlations in graphs while preserving CSEs (common sub expressions). Note that internal
     * aliases are not translated, only deeply-correlated aliases are translated according to the given
     * {@link TranslationMap}. The logic in this method delegates to
     * {@link RelationalExpression#translateCorrelations(TranslationMap, boolean, List)} to in turn translate
     * predicates, select list expressions and others. Furthermore, the logic in this method attempts to not copy
     * a sub-graph unless it is necessary to do so, i.e. uncorrelated sub-graphs are shared between the original graph
     * and the translated one.
     * @param refs a list of {@link Reference}s. They can share common sub-graphs
     * @param memoizer a {@link Memoizer}. If used outside the planner rules, use
     *        {@link Memoizer#noMemoization(PlannerStage)}
     * @param translationMap the {@link TranslationMap} that defines the translations to be carried out
     * @param shouldSimplifyValues an indicator whether we should attempt to simplify values after translation
     * @return a list of translated references
     */
    @Nonnull
    public static List<? extends Reference> translateCorrelationsInGraphs(@Nonnull final List<? extends Reference> refs,
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
                            Verify.verify(!translationMap.containsSourceAlias(alias),
                                    "internal quantifiers cannot be re-aliased");
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

    /**
     * Method to rebase a graph while preserving CSEs (common sub expressions). Note that internal
     * aliases are also translated as well as correlations are translated (rebased) according to the given
     * {@link TranslationMap}. The logic in this method delegates to
     * {@link RelationalExpression#translateCorrelations(TranslationMap, boolean, List)} to in turn translate
     * predicates, select list expressions and others. Furthermore, the logic in this method attempts to not copy
     * a sub-graph unless it is necessary to do so, i.e. uncorrelated sub-graphs are shared between the original graph
     * and the translated one.
     * @param refs a list of {@link Reference}s. They can share common sub-graphs
     * @param memoizer a {@link Memoizer}. If used outside the planner rules, use
     *        {@link Memoizer#noMemoization(PlannerStage)}
     * @param translationMap the {@link TranslationMap} that defines the translations to be carried out
     * @param shouldSimplifyValues an indicator whether we should attempt to simplify values after translation
     * @return a list of translated references
     */
    @Nonnull
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
                    sourceAlias -> translationMap.getTargetOrDefault(sourceAlias, sourceAlias));
        }

        return refs.stream()
                .map(ref -> translationsCache.getOrDefault(ref, ref))
                .collect(ImmutableList.toImmutableList());
    }

    private static void rebaseGraph(@Nonnull final Memoizer memoizer, @Nonnull final TranslationMap translationMap,
                                    final boolean shouldSimplifyValues, @Nonnull final Reference reference,
                                    @Nonnull final IdentityHashMap<Reference, Reference> translationsCache,
                                    @Nonnull final Function<CorrelationIdentifier, CorrelationIdentifier> quantifierAliasMapper) {
        Verify.verify(!reference.isExploring());
        var allMembersSame = true;
        final var translatedExploratoryExpressionsBuilder = ImmutableList.<RelationalExpression>builder();
        final var translatedFinalExpressionsBuilder = ImmutableList.<RelationalExpression>builder();
        for (final var expression : reference.getAllMemberExpressions()) {
            var allChildTranslationsSame = true;
            final var translatedQuantifiersBuilder = ImmutableList.<Quantifier>builder();
            for (final var quantifier : expression.getQuantifiers()) {
                final var childReference = quantifier.getRangesOver();

                final var translatedChildReference =
                        translationsCache.getOrDefault(childReference, childReference);

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
                    PlannerEventListeners.dispatchEvent(
                            new TranslateCorrelationsPlannerEvent(translatedExpression, PlannerEvent.Location.COUNT));
                    allMembersSame = false;
                }
            } else {
                translatedExpression = expression.translateCorrelations(translationMap, shouldSimplifyValues,
                        translatedQuantifiers);
                PlannerEventListeners.dispatchEvent(
                        new TranslateCorrelationsPlannerEvent(translatedExpression, PlannerEvent.Location.COUNT));
                allMembersSame = false;
            }
            if (reference.isFinal(expression)) {
                translatedFinalExpressionsBuilder.add(translatedExpression);
            }
            if (reference.isExploratory(expression)) {
                translatedExploratoryExpressionsBuilder.add(translatedExpression);
            }
        }
        if (!allMembersSame) {
            final var translatedExploratoryExpressions =
                    translatedExploratoryExpressionsBuilder.build();
            final var translatedFinalExpressions =
                    translatedFinalExpressionsBuilder.build();

            final Reference translatedReference;
            if (translatedExploratoryExpressions.isEmpty()) {
                // no exploratory expressions; just final ones
                translatedReference = memoizer.memoizeFinalExpressions(translatedFinalExpressions);
            } else if (translatedFinalExpressions.isEmpty()) {
                // no final expressions; just exploratory ones
                translatedReference = memoizer.memoizeExploratoryExpressions(translatedExploratoryExpressions);
            } else {
                // mix
                translatedReference = memoizer.memoizeExpressions(translatedExploratoryExpressions, translatedFinalExpressions);
            }
            translatedReference.inheritConstraintsFromOther(reference);
            
            translationsCache.put(reference, translatedReference);
        }
    }
}

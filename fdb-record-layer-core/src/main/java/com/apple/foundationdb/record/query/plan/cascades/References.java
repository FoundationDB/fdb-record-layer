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
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.TranslateCorrelationsPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
     *
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
     * The result of {@link #instantiate}.
     *
     * @param reference the root of the new graph
     * @param aliasMap the map from the aliases in the original graph to the ones in the new graph. It contains the
     *        aliases of the quantifiers in the graph, and also the aliases the graph is correlated to from outside.
     *        An outside alias that the graph does not use is not in the map.
     */
    public record Instantiation(@Nonnull Reference reference, @Nonnull AliasMap aliasMap) {
    }

    /**
     * Creates an independent instance of a graph, so that the instance can be placed in another graph (for example,
     * once for each call site of a pre-compiled function). The instance has fresh aliases for all quantifiers in the
     * graph, so that two instances do not clash in the same statement. It also shares no {@link Reference} with the
     * graph that is instantiated, so that planning one of them does not change the other, which makes it safe to
     * instantiate a graph that is shared between threads.
     *
     * <p>The graph is rebased with a {@link ToUniqueAliasesTranslationMap}, which copies every reference that has a
     * quantifier. This method also copies the references that have none (the leaves of the graph), as they would be
     * shared otherwise.
     *
     * @param reference the root {@link Reference} of the graph to instantiate
     * @return the {@link Instantiation}, i.e. the root of the new graph, and the aliases it uses for the quantifiers
     *         of the original graph
     */
    @Nonnull
    public static Instantiation instantiate(@Nonnull final Reference reference) {
        final PlannerStage plannerStage = reference.getPlannerStage();
        // The references of the instance are created in the planner stage of the root, so all references of the graph
        // must be in that stage.
        Debugger.sanityCheck(() -> Verify.verify(
                Traversal.withRoot(reference).getRefs().stream().allMatch(ref -> ref.getPlannerStage() == plannerStage),
                "all references of the graph must be in the planner stage %s", plannerStage));
        return instantiate(reference, Memoizer.noMemoization(plannerStage));
    }

    /**
     * Variant of {@link #instantiate(Reference)} that creates the references of the instance with the given
     * {@link Memoizer}. The instance is only guaranteed to be independent if the memoizer does not reuse existing
     * references, as {@link Memoizer#noMemoization(PlannerStage)} does.
     *
     * @param reference the root {@link Reference} of the graph to instantiate
     * @param memoizer the {@link Memoizer} to create the references with
     * @return the {@link Instantiation}
     */
    @Nonnull
    static Instantiation instantiate(@Nonnull final Reference reference, @Nonnull final Memoizer memoizer) {
        final var translationMap = new ToUniqueAliasesTranslationMap();
        final Reference instance = Iterables.getOnlyElement(
                rebaseGraphs(ImmutableList.of(reference), memoizer, translationMap, RebaseOptions.COPY));
        return new Instantiation(instance, translationMap.getSnapshotAliasMap());
    }

    /**
     * Rebases a graph while preserving common sub-expressions (CSEs). Also renames the aliases of the quantifiers
     * in the graph, and translates the correlations of its expressions, according to the given {@link TranslationMap}.
     * These correlations point to quantifiers in the graph or to aliases outside of it. Each expression translates them
     * in its predicates, select list expressions and others by delegating to
     * {@link RelationalExpression#translateCorrelations}.
     *
     * <p>This method attempts to not copy a sub-graph unless necessary. In particular, a {@link Reference} is shared
     * between the original graph and the rebased one if nothing in its sub-graph is translated. Note that such sharing
     * can be problematic if the graph outlives the planning operation that uses the rebased graph. Use
     * {@link #instantiate} in that case.
     *
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
        return rebaseGraphs(refs, memoizer, translationMap, RebaseOptions.of(shouldSimplifyValues));
    }

    /**
     * Options of the private rebase methods.
     *
     * @param simplify whether to attempt to simplify values after translation
     * @param copy whether to copy every reference of the graph, even if nothing in it is translated
     */
    private record RebaseOptions(boolean simplify, boolean copy) {
        static final RebaseOptions NONE = new RebaseOptions(false, false);
        static final RebaseOptions SIMPLIFY = new RebaseOptions(true, false);
        static final RebaseOptions COPY = new RebaseOptions(false, true);

        @Nonnull
        static RebaseOptions of(final boolean simplify) {
            return simplify ? SIMPLIFY : NONE;
        }
    }

    @Nonnull
    private static List<? extends Reference> rebaseGraphs(@Nonnull final List<? extends Reference> refs,
                                                          @Nonnull final Memoizer memoizer,
                                                          @Nonnull final TranslationMap translationMap,
                                                          @Nonnull final RebaseOptions options) {
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
            rebaseGraph(memoizer, translationMap, options, reference, translationsCache,
                    sourceAlias -> translationMap.getTargetMaybe(sourceAlias).orElse(sourceAlias));
        }

        return refs.stream()
                .map(ref -> translationsCache.getOrDefault(ref, ref))
                .collect(ImmutableList.toImmutableList());
    }

    private static void rebaseGraph(@Nonnull final Memoizer memoizer, @Nonnull final TranslationMap translationMap,
                                    final boolean shouldSimplifyValues, @Nonnull final Reference reference,
                                    @Nonnull final IdentityHashMap<Reference, Reference> translationsCache,
                                    @Nonnull final Function<CorrelationIdentifier, CorrelationIdentifier> quantifierAliasMapper) {
        rebaseGraph(memoizer, translationMap, RebaseOptions.of(shouldSimplifyValues), reference, translationsCache,
                quantifierAliasMapper);
    }

    /**
     * Rebases a single reference, given the translations of the references it depends on, and records the result in
     * the {@code translationsCache}. A reference that is not translated is not recorded, unless
     * {@link RebaseOptions#copy} is set.
     */
    private static void rebaseGraph(@Nonnull final Memoizer memoizer, @Nonnull final TranslationMap translationMap,
                                    @Nonnull final RebaseOptions options, @Nonnull final Reference reference,
                                    @Nonnull final IdentityHashMap<Reference, Reference> translationsCache,
                                    @Nonnull final Function<CorrelationIdentifier, CorrelationIdentifier> quantifierAliasMapper) {
        Verify.verify(!reference.isExploring());
        final boolean shouldSimplifyValues = options.simplify();
        boolean allMembersSame = true;
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
                    PlannerEventListeners.dispatchEvent(() ->
                            new TranslateCorrelationsPlannerEvent(translatedExpression, Location.COUNT));
                    allMembersSame = false;
                }
            } else {
                translatedExpression = expression.translateCorrelations(translationMap, shouldSimplifyValues,
                        translatedQuantifiers);
                PlannerEventListeners.dispatchEvent(() ->
                        new TranslateCorrelationsPlannerEvent(translatedExpression, Location.COUNT));
                allMembersSame = false;
            }
            if (reference.isFinal(expression)) {
                translatedFinalExpressionsBuilder.add(translatedExpression);
            }
            if (reference.isExploratory(expression)) {
                translatedExploratoryExpressionsBuilder.add(translatedExpression);
            }
        }
        if (options.copy() || !allMembersSame) {
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

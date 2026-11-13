/*
 * TranslateGraphTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.References.RebaseOptions;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.elementOf;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.explode;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullScanExpression;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.reference;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.resultColumn;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;
import static org.assertj.core.api.Assertions.assertThat;

public class TranslateGraphTest extends FDBRecordStoreQueryTestBase {
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rebaseGraphTest() {
        final var cascadesPlanner = setUp();
        var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
        final var explodeQun =
                explode(FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()),
                                "reviews"));

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);
        graphExpansionBuilder.addQuantifier(explodeQun);
        graphExpansionBuilder.addPredicate(
                new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(),
                        outerQun.getFlowedObjectType()), "name"),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

        final var explodeResultValue = elementOf(explodeQun);
        graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
        outerQun = Quantifier.forEach(Reference.initialOf(
                graphExpansionBuilder.build().buildSelect()));

        graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);

        var innerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantReviewer");
        graphExpansionBuilder.addQuantifier(innerQun);

        final var outerQuantifiedValue = QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType());
        final var innerQuantifiedValue = QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType());

        final var outerReviewerIdValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "reviewer"));
        final var innerReviewerIdValue = FieldValue.ofFieldName(innerQuantifiedValue, "id");

        graphExpansionBuilder.addPredicate(new ValuePredicate(innerReviewerIdValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, outerReviewerIdValue)));

        final var reviewerNameValue = FieldValue.ofFieldName(innerQuantifiedValue, "name");
        graphExpansionBuilder.addResultColumn(resultColumn(reviewerNameValue, "reviewerName"));
        final var reviewRatingValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "rating"));
        graphExpansionBuilder.addResultColumn(resultColumn(reviewRatingValue, "reviewRating"));

        final var qun = Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect()));
        final var expression = LogicalSortExpression.unsorted(qun);
        final var reference = Reference.initialOf(expression);
        final var translationMap = new ToUniqueAliasesTranslationMap();
        final var translatedReferences =
                References.rebaseGraphs(ImmutableList.of(reference),
                        Memoizer.noMemoization(PlannerStage.INITIAL),
                        translationMap);
        final Reference translatedReference = Iterables.getOnlyElement(translatedReferences);
        final var translatedExpression = translatedReference.get();

        // expression is still semantically the same (after the rebase)
        Assertions.assertTrue(expression.semanticEquals(translatedExpression));

        final var snapshotAliasMap = translationMap.getSnapshotAliasMap();
        Assertions.assertTrue(
                snapshotAliasMap.entrySet()
                        .stream()
                        .noneMatch(entry -> entry.getKey().equals(entry.getValue())));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rebaseGraphTestWithDiamond() {
        final var cascadesPlanner = setUp();
        final var metaData = cascadesPlanner.getRecordMetaData();
        final var fuseReference = reference(fullScanExpression(metaData));
        final var fullScanQuantifierForRestaurant = forEach(fuseReference);
        var outerQun = fullTypeScan(metaData, "RestaurantRecord", fullScanQuantifierForRestaurant);
        final var explodeQun =
                explode(FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()),
                                "reviews"));

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);
        graphExpansionBuilder.addQuantifier(explodeQun);
        graphExpansionBuilder.addPredicate(
                new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(),
                        outerQun.getFlowedObjectType()), "name"),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

        final var explodeResultValue = elementOf(explodeQun);
        graphExpansionBuilder.addResultColumn(resultColumn(explodeResultValue, "review"));
        outerQun = Quantifier.forEach(Reference.initialOf(
                graphExpansionBuilder.build().buildSelect()));

        graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);

        final var fullScanQuantifierForReviewer = forEach(fuseReference);
        var innerQun = fullTypeScan(metaData, "RestaurantReviewer", fullScanQuantifierForReviewer);
        graphExpansionBuilder.addQuantifier(innerQun);

        final var outerQuantifiedValue = QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType());
        final var innerQuantifiedValue = QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType());

        final var outerReviewerIdValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "reviewer"));
        final var innerReviewerIdValue = FieldValue.ofFieldName(innerQuantifiedValue, "id");

        graphExpansionBuilder.addPredicate(new ValuePredicate(innerReviewerIdValue, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, outerReviewerIdValue)));

        final var reviewerNameValue = FieldValue.ofFieldName(innerQuantifiedValue, "name");
        graphExpansionBuilder.addResultColumn(resultColumn(reviewerNameValue, "reviewerName"));
        final var reviewRatingValue = FieldValue.ofFieldNames(outerQuantifiedValue, ImmutableList.of("review", "rating"));
        graphExpansionBuilder.addResultColumn(resultColumn(reviewRatingValue, "reviewRating"));

        final var qun = Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect()));
        final var expression = LogicalSortExpression.unsorted(qun);
        final var reference = Reference.initialOf(expression);
        final var translationMap = new ToUniqueAliasesTranslationMap();
        final var translatedReferences =
                References.rebaseGraphs(ImmutableList.of(reference),
                        Memoizer.noMemoization(PlannerStage.INITIAL),
                        translationMap);
        final Reference translatedReference = Iterables.getOnlyElement(translatedReferences);
        final var translatedExpression = translatedReference.get();

        // expression is still semantically the same (after the rebase)
        Assertions.assertTrue(expression.semanticEquals(translatedExpression));

        final var snapshotAliasMap = translationMap.getSnapshotAliasMap();
        Assertions.assertTrue(
                snapshotAliasMap.entrySet()
                        .stream()
                        .noneMatch(entry -> entry.getKey().equals(entry.getValue())));
        final var translatedQuantifierMap =
                Quantifiers.aliasToQuantifierMap(collectQuantifiers(translatedReference));

        // both special quantifiers over the CSE are mapped
        Assertions.assertTrue(snapshotAliasMap.containsSource(fullScanQuantifierForRestaurant.getAlias()));
        Assertions.assertTrue(snapshotAliasMap.containsSource(fullScanQuantifierForReviewer.getAlias()));

        // diamond shape must be preserved
        Assertions.assertSame(translatedQuantifierMap.get(snapshotAliasMap.getTarget(fullScanQuantifierForRestaurant.getAlias())).getRangesOver(),
                translatedQuantifierMap.get(snapshotAliasMap.getTarget(fullScanQuantifierForReviewer.getAlias())).getRangesOver());
    }

    /**
     * Tests that rebasing a graph with the {@code COPY} option copies every reference of the graph, including the ones
     * that are not correlated to anything that is translated, and that it keeps the shape of the graph intact.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rebaseGraphWithoutSharingReferencesTest() {
        final var metaData = setUp().getRecordMetaData();
        final var fullScanReference = reference(fullScanExpression(metaData));
        final var restaurantQun = fullTypeScan(metaData, "RestaurantRecord", forEach(fullScanReference));
        final var reviewerQun = fullTypeScan(metaData, "RestaurantReviewer", forEach(fullScanReference));

        final var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(restaurantQun);
        graphExpansionBuilder.addQuantifier(reviewerQun);
        graphExpansionBuilder.addResultColumn(resultColumn(
                QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "restaurant"));
        graphExpansionBuilder.addResultColumn(resultColumn(
                QuantifiedObjectValue.of(reviewerQun.getAlias(), reviewerQun.getFlowedObjectType()), "reviewer"));
        final var expression = LogicalSortExpression.unsorted(
                Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect())));
        final var reference = Reference.initialOf(expression);

        final var translatedReference = Iterables.getOnlyElement(References.rebaseGraphs(
                ImmutableList.of(reference), Memoizer.noMemoization(PlannerStage.INITIAL),
                new ToUniqueAliasesTranslationMap(), RebaseOptions.COPY));

        assertThat(expression.semanticEquals(translatedReference.get())).isTrue();

        final var originalReferences = collectReferences(reference);
        final var translatedReferences = collectReferences(translatedReference);
        assertThat(originalReferences).contains(fullScanReference);
        assertThat(translatedReferences).doesNotContainAnyElementsOf(originalReferences);
        // Verify that the diamond over the full scan is preserved by the copy.
        assertThat(translatedReferences).hasSameSizeAs(originalReferences);
    }

    /**
     * Tests that rebasing a graph with the {@code COPY} option and a translation map that only defines identities
     * copies the references below an outer join.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rebaseGraphWithOuterJoinAndIdentityTranslationMapTest() {
        final var metaData = setUp().getRecordMetaData();
        final var preservedQun = fullTypeScan(metaData, "RestaurantRecord").narrow(Quantifier.ForEach.class);
        final var nullSupplyingQun = fullTypeScan(metaData, "RestaurantReviewer").narrow(Quantifier.ForEach.class);
        final var outerJoin = new OuterJoinExpression(preservedQun, nullSupplyingQun, ImmutableList.of(),
                QuantifiedObjectValue.of(preservedQun.getAlias(), preservedQun.getFlowedObjectType()));
        final var reference = Reference.initialOf(outerJoin);

        final var translatedReference = Iterables.getOnlyElement(References.rebaseGraphs(
                ImmutableList.of(reference), Memoizer.noMemoization(PlannerStage.INITIAL), TranslationMap.empty(),
                RebaseOptions.COPY));

        assertThat(collectReferences(translatedReference)).doesNotContainAnyElementsOf(collectReferences(reference));
    }

    /**
     * Tests that translating the correlations in a graph re-targets the deeply-correlated alias, while the
     * uncorrelated sub-graph is shared between the original graph and the translated one.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void translateCorrelationsInGraphsTest1() {
        final var metaData = setUp().getRecordMetaData();
        final var sourceQun = fullTypeScan(metaData, "RestaurantRecord");
        final var sourceAlias = sourceQun.getAlias();
        final var targetAlias = CorrelationIdentifier.uniqueId();

        final var innerQun = fullTypeScan(metaData, "RestaurantReviewer");
        final var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(innerQun);
        graphExpansionBuilder.addPredicate(new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType()), "name"),
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                        FieldValue.ofFieldName(QuantifiedObjectValue.of(sourceAlias, sourceQun.getFlowedObjectType()), "name"))));
        graphExpansionBuilder.addResultColumn(resultColumn(
                QuantifiedObjectValue.of(innerQun.getAlias(), innerQun.getFlowedObjectType()), "reviewer"));
        final var reference = Reference.initialOf(graphExpansionBuilder.build().buildSelect());
        assertThat(reference.getCorrelatedTo()).containsExactly(sourceAlias);

        final var translatedReference = reference.translateGraph(Memoizer.noMemoization(PlannerStage.INITIAL),
                TranslationMap.ofAliases(sourceAlias, targetAlias), false);

        assertThat(translatedReference).isNotSameAs(reference);
        assertThat(translatedReference.getCorrelatedTo()).containsExactly(targetAlias);
        // The full scan below the select is not correlated to the translated alias, so it is shared.
        assertThat(collectReferences(translatedReference)).contains(innerQun.getRangesOver());
    }

    /**
     * Tests that translating the correlations in a graph that is not correlated to the translated alias returns the
     * very same references.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void translateCorrelationsInGraphsTest2() {
        final var metaData = setUp().getRecordMetaData();
        final var reference = fullTypeScan(metaData, "RestaurantRecord").getRangesOver();

        final var translatedReference = reference.translateGraph(Memoizer.noMemoization(PlannerStage.INITIAL),
                TranslationMap.ofAliases(CorrelationIdentifier.uniqueId(), CorrelationIdentifier.uniqueId()), false);

        assertThat(translatedReference).isSameAs(reference);
        assertThat(References.translateCorrelationsInGraphs(ImmutableList.of(),
                Memoizer.noMemoization(PlannerStage.INITIAL), TranslationMap.empty(), false)).isEmpty();
    }

    @Nonnull
    private Set<Reference> collectReferences(@Nonnull Reference reference) {
        final var partialOrder = referencesAndDependencies().evaluate(reference);
        final Set<Reference> references = Sets.newIdentityHashSet();
        references.addAll(TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                .orElseThrow(() -> new RecordCoreException("graph has cycles")));
        return references;
    }

    @Nonnull
    private Set<Quantifier> collectQuantifiers(@Nonnull Reference reference) {
        final var partialOrder = referencesAndDependencies().evaluate(reference);
        final var references =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles"));
        return references.stream()
                .flatMap(r -> r.getFinalExpressions().stream())
                .flatMap(expression -> expression.getQuantifiers().stream())
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
    }

    @Nonnull
    private CascadesPlanner setUp() {
        final CascadesPlanner cascadesPlanner;

        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
            cascadesPlanner = (CascadesPlanner)planner;
            commit(context);
        }
        return cascadesPlanner;
    }
}

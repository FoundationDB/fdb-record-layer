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
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                        translationMap, false);
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
                        translationMap, false);
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
     * Tests that instantiating a graph copies every reference of the graph, including the leaf that is shared by two
     * quantifiers, keeps the shape of the graph intact, and renames the aliases of all quantifiers.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void instantiateGraphWithDiamond() {
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

        final var instantiation = References.instantiate(reference);
        final var instance = instantiation.reference();

        assertThat(expression.semanticEquals(instance.get())).isTrue();

        final var originalReferences = collectReferences(reference);
        final var instanceReferences = collectReferences(instance);
        assertThat(originalReferences).contains(fullScanReference);
        assertThat(instanceReferences).doesNotContainAnyElementsOf(originalReferences);
        // Verify that the diamond over the full scan is preserved by the copy.
        assertThat(instanceReferences).hasSameSizeAs(originalReferences);

        final var aliasMap = instantiation.aliasMap();
        for (final var quantifier : collectQuantifiers(reference)) {
            assertThat(aliasMap.containsSource(quantifier.getAlias())).isTrue();
            assertThat(aliasMap.getTarget(quantifier.getAlias())).isNotEqualTo(quantifier.getAlias());
        }
    }

    /**
     * Tests that two instances of the same graph have no reference and no quantifier alias in common.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void instantiateGraphTwice() {
        final var metaData = setUp().getRecordMetaData();
        final var reference = Reference.initialOf(LogicalSortExpression.unsorted(
                fullTypeScan(metaData, "RestaurantRecord")));

        final var instance1 = References.instantiate(reference).reference();
        final var instance2 = References.instantiate(reference).reference();

        assertThat(collectReferences(instance1)).doesNotContainAnyElementsOf(collectReferences(instance2));
        assertThat(collectQuantifiers(instance1).stream().map(Quantifier::getAlias))
                .doesNotContainAnyElementsOf(collectQuantifiers(instance2).stream().map(Quantifier::getAlias).toList());
    }

    /**
     * Tests that instantiating a graph whose references are not all in the same planner stage fails.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void instantiateGraphWithMixedPlannerStages() {
        final var metaData = setUp().getRecordMetaData();
        final var innerQun = fullTypeScan(metaData, "RestaurantRecord");
        innerQun.getRangesOver().advancePlannerStageUnchecked(PlannerStage.CANONICAL);
        final var reference = Reference.initialOf(LogicalSortExpression.unsorted(innerQun));

        assertThatThrownBy(() -> References.instantiate(reference)).isInstanceOf(VerifyException.class);
    }

    /**
     * Tests that instantiating a graph copies references that have exploratory members only and references that have
     * both exploratory and final members, and keeps the members of each kind.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void instantiateGraphWithExploratoryMembers() {
        final var metaData = setUp().getRecordMetaData();
        final var exploratoryOnlyReference =
                Reference.ofExploratoryExpression(PlannerStage.INITIAL, fullScanExpression(metaData));
        final var mixedReference = Reference.of(PlannerStage.INITIAL,
                ImmutableList.of(fullScanExpression(metaData)), ImmutableList.of(fullScanExpression(metaData)));
        final var restaurantQun = fullTypeScan(metaData, "RestaurantRecord", forEach(exploratoryOnlyReference));
        final var reviewerQun = fullTypeScan(metaData, "RestaurantReviewer", forEach(mixedReference));

        final var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(restaurantQun);
        graphExpansionBuilder.addQuantifier(reviewerQun);
        graphExpansionBuilder.addResultColumn(resultColumn(
                QuantifiedObjectValue.of(restaurantQun.getAlias(), restaurantQun.getFlowedObjectType()), "restaurant"));
        final var reference = Reference.initialOf(graphExpansionBuilder.build().buildSelect());

        final var instanceReferences = collectReferences(References.instantiate(reference).reference());

        assertThat(instanceReferences).doesNotContainAnyElementsOf(collectReferences(reference));
        assertThat(instanceReferences).filteredOn(ref -> ref.getExploratoryExpressions().size() == 1
                        && ref.getFinalExpressions().isEmpty())
                .hasSize(1);
        assertThat(instanceReferences).filteredOn(ref -> ref.getExploratoryExpressions().size() == 1
                        && ref.getFinalExpressions().size() == 1)
                .hasSize(1);
    }

    /**
     * Tests that the instance of a graph inherits the constraints of the reference it is created from, and that
     * pushing a constraint into the instance does not change the constraints of the original.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void instantiateGraphDoesNotShareConstraints() {
        final var metaData = setUp().getRecordMetaData();
        final var reference = Reference.initialOf(LogicalSortExpression.unsorted(
                fullTypeScan(metaData, "RestaurantRecord")));
        reference.getConstraintsMap().pushProperty(RequestedOrderingConstraint.REQUESTED_ORDERING,
                ImmutableSet.of(RequestedOrdering.preserve()));

        final var instance = References.instantiate(reference).reference();
        assertThat(instance.getConstraintsMap().getConstraintOptional(RequestedOrderingConstraint.REQUESTED_ORDERING))
                .hasValueSatisfying(orderings -> assertThat(orderings).hasSize(1));

        instance.getConstraintsMap().pushProperty(RequestedOrderingConstraint.REQUESTED_ORDERING,
                ImmutableSet.of(RequestedOrdering.ofParts(ImmutableList.of(), RequestedOrdering.Distinctness.DISTINCT,
                        false, ImmutableSet.of())));

        assertThat(instance.getConstraintsMap().getConstraintOptional(RequestedOrderingConstraint.REQUESTED_ORDERING))
                .hasValueSatisfying(orderings -> assertThat(orderings).hasSize(2));
        assertThat(reference.getConstraintsMap().getConstraintOptional(RequestedOrderingConstraint.REQUESTED_ORDERING))
                .hasValueSatisfying(orderings -> assertThat(orderings).hasSize(1));
    }

    /**
     * Tests that translating the correlations of an outer join with a translation map that only defines identities
     * returns the outer join itself if its quantifiers are the same, and a new one if a quantifier is over another
     * reference (but has the same alias).
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void translateCorrelationsOfOuterJoinWithIdentityMap() {
        final var metaData = setUp().getRecordMetaData();
        final var preservedQun = fullTypeScan(metaData, "RestaurantRecord").narrow(Quantifier.ForEach.class);
        final var nullSupplyingQun = fullTypeScan(metaData, "RestaurantReviewer").narrow(Quantifier.ForEach.class);
        final var outerJoin = new OuterJoinExpression(preservedQun, nullSupplyingQun, ImmutableList.of(),
                QuantifiedObjectValue.of(preservedQun.getAlias(), preservedQun.getFlowedObjectType()));

        assertThat(outerJoin.translateCorrelations(TranslationMap.empty(), false,
                ImmutableList.of(preservedQun, nullSupplyingQun))).isSameAs(outerJoin);

        final var copiedPreservedQun = preservedQun.overNewReference(reference(fullScanExpression(metaData)));
        final var translatedOuterJoin = outerJoin.translateCorrelations(TranslationMap.empty(), false,
                ImmutableList.of(copiedPreservedQun, nullSupplyingQun));
        assertThat(translatedOuterJoin).isNotSameAs(outerJoin);
        assertThat(translatedOuterJoin.getQuantifiers().get(0)).isSameAs(copiedPreservedQun);
    }

    /**
     * Tests that translating the correlations in a graph re-targets the alias that the graph is correlated to, while
     * the sub-graph that is not correlated to it is shared between the original graph and the translated one.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void translateCorrelationsInGraphsTest() {
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
        assertThat(collectReferences(translatedReference)).contains(innerQun.getRangesOver());
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

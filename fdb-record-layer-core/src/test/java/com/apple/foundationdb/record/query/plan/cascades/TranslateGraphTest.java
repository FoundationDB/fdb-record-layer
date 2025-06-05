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
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullScanExpression;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.reference;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.resultColumn;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;

public class TranslateGraphTest extends FDBRecordStoreQueryTestBase {
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rebaseGraphTest() {
        final var cascadesPlanner = setUp();
        var outerQun = fullTypeScan(cascadesPlanner.getRecordMetaData(), "RestaurantRecord");
        final var explodeQun =
                Quantifier.forEach(Reference.initialOf(
                        new ExplodeExpression(FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()),
                                "reviews"))));

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);
        graphExpansionBuilder.addQuantifier(explodeQun);
        graphExpansionBuilder.addPredicate(
                new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(),
                        outerQun.getFlowedObjectType()), "name"),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

        final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
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
                Quantifier.forEach(Reference.initialOf(
                        new ExplodeExpression(FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(outerQun.getAlias(), outerQun.getFlowedObjectType()),
                                "reviews"))));

        var graphExpansionBuilder = GraphExpansion.builder();
        graphExpansionBuilder.addQuantifier(outerQun);
        graphExpansionBuilder.addQuantifier(explodeQun);
        graphExpansionBuilder.addPredicate(
                new ValuePredicate(FieldValue.ofFieldName(QuantifiedObjectValue.of(outerQun.getAlias(),
                        outerQun.getFlowedObjectType()), "name"),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "name")));

        final var explodeResultValue = QuantifiedObjectValue.of(explodeQun.getAlias(), explodeQun.getFlowedObjectType());
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

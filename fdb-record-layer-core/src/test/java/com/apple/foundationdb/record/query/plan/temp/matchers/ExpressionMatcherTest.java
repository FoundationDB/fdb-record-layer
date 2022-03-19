/*
 * ExpressionMatcherTest.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.predicates.QueryComponentPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Check that expression matchers are able to properly match planner expressions and references using the
 * {@link BindingMatcher#bindMatches(PlannerBindings, Object)} method.
 * These tests rely on dereferencing references in a number of places since we use equality checking to make sure that
 * the bindings are returning the correct values. Technically, this violates the contract (that planner expression
 * children might not be present, might be masked, etc.). This test might break in the future if that were to happen.
 */
public class ExpressionMatcherTest {
    private static final List<BindingMatcher<? extends RecordQueryPlan>> existingMatchers = ImmutableList.of(
            RecordQueryPlanMatchers.indexPlan(),
            RelationalExpressionMatchers.ofType(RecordQueryPlan.class));
    private static final List<? extends RecordQueryPlan> existingBindables = ImmutableList.of(
            new RecordQueryIndexPlan("fake_index", IndexScanComparisons.byValue(), false),
            new RecordQueryScanPlan(ScanComparisons.EMPTY, false));

    @Nonnull
    private PlannerBindings getExistingBindings() {
        PlannerBindings.Builder bindings = PlannerBindings.newBuilder();
        for (int i = 0; i < existingMatchers.size(); i++) {
            bindings.put(existingMatchers.get(i), existingBindables.get(i));
        }
        return bindings.build();
    }

    private void assertExistingBindingsSurvived(@Nonnull PlannerBindings bindings) {
        for (int i = 0; i < existingMatchers.size(); i++) {
            assertTrue(bindings.containsKey(existingMatchers.get(i)));
            assertEquals(existingBindables.get(i), bindings.get(existingMatchers.get(i)));
        }
    }

    @Test
    public void anyRefMatcher() {
        // create a matcher and expression to match
        BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> matcher = ReferenceMatchers.anyRef();
        Quantifier.ForEach quantifier = Quantifier.forEach(GroupExpressionRef.of(new RecordQueryScanPlan(ScanComparisons.EMPTY, false)));
        ExpressionRef<RelationalExpression> root = GroupExpressionRef.of(
                new LogicalFilterExpression(
                        ImmutableList.of(new QueryComponentPredicate(Query.field("test").equalsValue(5))),
                        quantifier));
        // try to match to expression
        Optional<PlannerBindings> newBindings = matcher.bindMatches(PlannerBindings.empty(), root).findFirst();
        // check the the bindings are what we expect, and that none of the existing ones were clobbered
        assertTrue(newBindings.isPresent());
        PlannerBindings allBindings = newBindings.get().mergedWith(getExistingBindings());
        assertExistingBindingsSurvived(allBindings);
        assertTrue(newBindings.get().containsKey(matcher));
        assertTrue(allBindings.containsKey(matcher));
        assertEquals(root, allBindings.get(matcher));
    }

    @Test
    public void singleTypeMatcher() {
        // we already have a different RecordQueryIndexPlan matcher, but this should still work
        BindingMatcher<RecordQueryIndexPlan> matcher = RecordQueryPlanMatchers.indexPlan();
        final IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        final ExpressionRef<RelationalExpression> root =
                GroupExpressionRef.of(new RecordQueryIndexPlan("an_index", fullValueScan, true));
        Optional<PlannerBindings> newBindings = matcher.bindMatches(PlannerBindings.empty(), root.get()).findFirst();
        // check the the bindings are what we expect, and that none of the existing ones were clobbered
        assertTrue(newBindings.isPresent());
        PlannerBindings allBindings = newBindings.get().mergedWith(getExistingBindings());
        assertExistingBindingsSurvived(allBindings);
        assertTrue(newBindings.get().containsKey(matcher));
        assertTrue(allBindings.containsKey(matcher));
        RecordQueryIndexPlan matched = allBindings.get(matcher);
        assertEquals(root.get(), matched);
    }

    @Test
    public void nestedTypeMatchers() {
        BindingMatcher<RecordQueryIndexPlan> childMatcher1 = RecordQueryPlanMatchers.indexPlan();
        BindingMatcher<RecordQueryScanPlan> childMatcher2 = RecordQueryPlanMatchers.scanPlan();
        BindingMatcher<RecordQueryUnionPlan> parentMatcher = RecordQueryPlanMatchers.union(
                ListMatcher.exactly(QuantifierMatchers.physicalQuantifier(childMatcher1),
                        QuantifierMatchers.physicalQuantifier(childMatcher2)));
        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        RecordQueryIndexPlan child1 = new RecordQueryIndexPlan("an_index", fullValueScan, true);
        RecordQueryScanPlan child2 = new RecordQueryScanPlan(ScanComparisons.EMPTY, true);

        // check matches if the children are in the right order
        RelationalExpression root = RecordQueryUnionPlan.from( // union with arbitrary comparison key
                child1, child2, EmptyKeyExpression.EMPTY, false);
        Optional<PlannerBindings> possibleBindings = parentMatcher.bindMatches(PlannerBindings.empty(), root).findFirst();
        assertTrue(possibleBindings.isPresent());
        PlannerBindings allBindings = possibleBindings.get().mergedWith(getExistingBindings());
        assertExistingBindingsSurvived(allBindings);
        assertEquals(root, allBindings.get(parentMatcher));
        assertEquals(child1, allBindings.get(childMatcher1));
        assertEquals(child2, allBindings.get(childMatcher2));

        // check that we fail to match if the children are in the wrong order
        root = RecordQueryUnionPlan.from( // union with arbitrary comparison key
                child2, child1, EmptyKeyExpression.EMPTY, false);
        assertFalse(parentMatcher.bindMatches(PlannerBindings.empty(), root).findFirst().isPresent());
    }

    @Test
    public void matchChildOrder() {
        BindingMatcher<RecordQueryUnionPlan> parentMatcher = RecordQueryPlanMatchers.union(
                ListMatcher.exactly(QuantifierMatchers.physicalQuantifier(RecordQueryPlanMatchers.indexPlan()),
                        QuantifierMatchers.physicalQuantifier(RecordQueryPlanMatchers.scanPlan())));

        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        RecordQueryIndexPlan child1 = new RecordQueryIndexPlan("an_index", fullValueScan, true);
        RecordQueryScanPlan child2 = new RecordQueryScanPlan(ScanComparisons.EMPTY, true);
        RelationalExpression root = RecordQueryUnionPlan.from( // union with arbitrary comparison key
                child1, child2, EmptyKeyExpression.EMPTY, false);
        assertTrue(parentMatcher.bindMatches(PlannerBindings.empty(), root).findFirst().isPresent());

        root = RecordQueryUnionPlan.from( // union with arbitrary comparison key
                child2, child1, EmptyKeyExpression.EMPTY, false);
        assertFalse(parentMatcher.bindMatches(PlannerBindings.empty(), root).findFirst().isPresent());
    }

    @Test
    public void matchChildrenAsReferences() {
        BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> childMatcher1 = ReferenceMatchers.anyRef();
        BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> childMatcher2 = ReferenceMatchers.anyRef();
        BindingMatcher<RecordQueryUnionPlan> matcher = RecordQueryPlanMatchers.union(
                ListMatcher.exactly(QuantifierMatchers.physicalQuantifierOverRef(childMatcher1),
                        QuantifierMatchers.physicalQuantifierOverRef(childMatcher2)));

        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        RecordQueryIndexPlan child1 = new RecordQueryIndexPlan("an_index", fullValueScan, true);
        RecordQueryScanPlan child2 = new RecordQueryScanPlan(ScanComparisons.EMPTY, true);
        RelationalExpression root = RecordQueryUnionPlan.from( // union with arbitrary comparison key
                child1, child2, EmptyKeyExpression.EMPTY, false);

        Optional<PlannerBindings> possibleBindings = matcher.bindMatches(PlannerBindings.empty(), root).findFirst();
        assertTrue(possibleBindings.isPresent());
        PlannerBindings newBindings = possibleBindings.get().mergedWith(getExistingBindings());
        assertExistingBindingsSurvived(newBindings);
        assertEquals(root, newBindings.get(matcher)); // check that root matches
        // check that children are behind references
        assertEquals(child1, newBindings.get(childMatcher1).get());
        assertEquals(child2, newBindings.get(childMatcher2).get());
    }
    

    @Test
    public void treeDescentWithMixedBindings() {
        // build a relatively complicated matcher
        BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> filterLeafMatcher = ReferenceMatchers.anyRef();
        BindingMatcher<QueryPredicate> predicateMatcher = QueryPredicateMatchers.anyPredicate();
        final BindingMatcher<LogicalFilterExpression> filterPlanMatcher =
                RelationalExpressionMatchers.logicalFilterExpression(MultiMatcher.AllMatcher.all(predicateMatcher), AnyMatcher.any(QuantifierMatchers.forEachQuantifierOverRef(filterLeafMatcher)));

        BindingMatcher<RecordQueryScanPlan> scanMatcher = RecordQueryPlanMatchers.scanPlan();
        BindingMatcher<LogicalUnionExpression> matcher = RelationalExpressionMatchers.logicalUnionExpression(
                ListMatcher.exactly(QuantifierMatchers.forEachQuantifier(filterPlanMatcher),
                        QuantifierMatchers.forEachQuantifier(scanMatcher)));

        // build a relatively complicated expression
        QueryComponent andBranch1 = Query.field("field1").greaterThan(6);
        QueryComponent andBranch2 = Query.field("field2").equalsParameter("param");
        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        final var baseRef = GroupExpressionRef.of(new RecordQueryIndexPlan("an_index", fullValueScan, true));
        final Quantifier.ForEach quantifier = Quantifier.forEach(baseRef);
        LogicalFilterExpression filterPlan =
                new LogicalFilterExpression(Query.and(andBranch1, andBranch2).expand(quantifier, () -> Quantifier.forEach(baseRef)).getPredicates(),
                        quantifier);
        RecordQueryScanPlan scanPlan = new RecordQueryScanPlan(ScanComparisons.EMPTY, true);
        RelationalExpression root =
                new LogicalUnionExpression(
                        Quantifiers.forEachQuantifiers(ImmutableList.of(GroupExpressionRef.of(filterPlan),
                                GroupExpressionRef.of(scanPlan))));

        assertTrue(filterPlanMatcher.bindMatches(PlannerBindings.empty(), filterPlan).findFirst().isPresent());

        // try to bind
        Optional<PlannerBindings> possibleBindings = matcher.bindMatches(PlannerBindings.empty(), root).findFirst();
        // check that all the bindings match what we expect
        assertTrue(possibleBindings.isPresent());
        PlannerBindings bindings = possibleBindings.get().mergedWith(getExistingBindings());
        assertEquals(root, bindings.get(matcher));
        assertEquals(filterPlan, bindings.get(filterPlanMatcher));
        assertEquals(scanPlan, bindings.get(scanMatcher));
        assertEquals(filterPlan.getPredicates(), bindings.getAll(predicateMatcher));
        assertEquals(filterPlan.getInner().getRangesOver().get(), bindings.get(filterLeafMatcher).get()); // dereference
    }
}

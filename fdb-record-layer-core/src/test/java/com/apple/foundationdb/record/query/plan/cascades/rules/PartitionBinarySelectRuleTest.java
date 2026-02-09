/*
 * PartitionBinarySelectRuleTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.explodeField;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;

public class PartitionBinarySelectRuleTest {
    // Single RCV containing a single column. Values of this sort are inserted by the rule of predicates are pushed down
    // into a rule when the final result value isn't used later
    private static final Value RCV_OF_ONE = RecordConstructorValue.ofUnnamed(ImmutableList.of(LiteralValue.ofScalar(1)));

    private final RuleTestHelper testHelper = new RuleTestHelper(new PartitionBinarySelectRule());

    //
    // Un-correlated joins.
    //
    // The two sides of the join are on independent quantifiers. Predicates can be transferred
    // to the two sides as needed
    //

    @Test
    void partitionSimpleSelect() {
        // A binary select with each predicate isolated to each side. Predicates should be isolated to the
        // one that the predicate is on

        final Quantifier t = baseT();
        final Quantifier tau = baseTau();

        final SelectExpression join = join(t, tau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello")))
                .addPredicate(fieldPredicate(tau, "beta", new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "world")))
                .build().buildSelect();

        final Quantifier newLeft = forEach(
                selectWithPredicates(t, fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello")))
        );
        final Quantifier newRight = forEach(
                selectWithPredicates(tau, fieldPredicate(tau, "beta", new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "world")))
        );
        final SelectExpression newJoin = join(newLeft, newRight)
                .addResultColumn(projectColumn(newLeft, "a"))
                .addResultColumn(projectColumn(newRight, "alpha"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }

    @Test
    void pushSimpleJoinCriterionBothSides() {
        // A binary select with one join predicate. We should create candidates pushing the join to both
        // the left and right

        final Quantifier t = baseT();
        final Quantifier tau = baseTau();
        final QueryPredicate joinPredicate = fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta")));
        final SelectExpression join = join(t, tau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .addPredicate(joinPredicate)
                .build().buildSelect();

        // Push to the t quantifier
        final Quantifier newT = forEach(
                FDBQueryGraphTestHelpers.selectWithPredicates(t, joinPredicate));
        final SelectExpression newJoinOnT = join(tau, newT)
                .addResultColumn(projectColumn(newT, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .build().buildSelect();

        // Push to the tau quantifier
        final Quantifier newTau = forEach(
                FDBQueryGraphTestHelpers.selectWithPredicates(tau, joinPredicate));
        final SelectExpression newJoinOnTau = join(t, newTau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(newTau, "alpha"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoinOnT, newJoinOnTau);
    }

    @Test
    void pushUncorrelatedPredicateOppositeToJoin() {
        // A binary select with a predicate that is not correlated to either side. We want it to
        // be pushed to the opposite side of the join predicate. This will allow the join to be planned
        // with the side with the uncorrelated predicate as the outer and the correlated side as the inner

        final Quantifier t = baseT();
        final Quantifier tau = baseTau();
        final QueryPredicate joinPredicate = fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta")));
        final QueryPredicate uncorrelatedPredicate = new ValuePredicate(LiteralValue.ofScalar(42L),
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                        ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.LONG, false))));
        final SelectExpression join = join(t, tau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .addPredicate(joinPredicate)
                .addPredicate(uncorrelatedPredicate)
                .build().buildSelect();

        // Join criterion pushed to t; uncorrelated predicate to tau
        final Quantifier newTau1 = forEach(selectWithPredicates(tau, uncorrelatedPredicate));
        final Quantifier newT1 = forEach(selectWithPredicates(t,
                fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(newTau1, "beta")))));
        final SelectExpression newJoin1 = join(newTau1, newT1)
                .addResultColumn(projectColumn(newT1, "a"))
                .addResultColumn(projectColumn(newTau1, "alpha"))
                .build().buildSelect();

        // Join criterion pushed to tau; uncorrelated predicate to t
        final Quantifier newT2 = forEach(selectWithPredicates(t, uncorrelatedPredicate));
        final Quantifier newTau2 = forEach(selectWithPredicates(tau,
                fieldPredicate(newT2, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta")))));
        final SelectExpression newJoin2 = join(newT2, newTau2)
                .addResultColumn(projectColumn(newT2, "a"))
                .addResultColumn(projectColumn(newTau2, "alpha"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin1, newJoin2);
    }

    @Test
    void partitionWhenOneSideNotInResultValue() {
        // Partition a binary join when one side does not appear in the result value. The side that does
        // not appear effectively only filters out results from the side that does appear

        final Quantifier t = baseT();
        final Quantifier tau = baseTau();
        final QueryPredicate joinPredicate = fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta")));
        final SelectExpression join = join(t, tau)
                .addResultColumn(projectColumn(t, "a"))
                .addPredicate(joinPredicate)
                .build().buildSelect();

        // Join criterion pushed to t
        final Quantifier newT = forEach(selectWithPredicates(t, joinPredicate));
        final SelectExpression newJoin1 = join(newT, tau)
                .addResultColumn(projectColumn(newT, "a"))
                .build().buildSelect();

        // Join criterion pushed to tau. As tau does not appear in the result value, the value 1 is projected
        final Quantifier newTau = forEach(new SelectExpression(RCV_OF_ONE, ImmutableList.of(tau), ImmutableList.of(joinPredicate)));
        final SelectExpression newJoin2 = join(t, newTau)
                .addResultColumn(projectColumn(t, "a"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin1, newJoin2);
    }

    //
    // Correlated joins.
    //
    // One side of the join is correlated to the other (via an ExplodeExpression), which limits the
    // set of legal transformations
    //

    @Test
    void pushSimplePredicatesWithCorrelatedQuantifiers() {
        // We have a simple binary join with two quantifiers, one of which is correlated to the other. The predicates
        // are each on one quantifier, so they should just be pushed to the appropriate side

        final Quantifier t = baseT();
        final Quantifier g = explodeField(t, "g");
        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(g, "one"))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "hello")))
                .addPredicate(fieldPredicate(g, "two", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "world")))
                .build().buildSelect();

        // Push the predicate on to the t quantifier and the predicate on g onto the g quantifier
        final Quantifier newT = forEach(selectWithPredicates(t, fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "hello"))));
        final Quantifier newG = forEach(selectWithPredicates(g, fieldPredicate(g, "two", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "world"))));
        final SelectExpression newJoin = join(newT, newG)
                .addResultColumn(projectColumn(newT, "a"))
                .addResultColumn(projectColumn(newG, "one"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }

    @Test
    void pushCorrelatedPredicateWithQunCorrelation() {
        // In this case, the two quantifiers are correlated. We can push the join criteria
        // only onto the one that the correlation points from

        final Quantifier t = baseT();
        final Quantifier g = explodeField(t, "g");
        final QueryPredicate joinPredicate = fieldPredicate(t, "b",
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(g, "two")));
        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(g, "one"))
                .addPredicate(joinPredicate)
                .build().buildSelect();

        // Can push the predicate down onto the g quantifier
        final Quantifier newG = forEach(selectWithPredicates(g, joinPredicate));
        final SelectExpression newJoin = join(t, newG)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(newG, "one"))
                .build().buildSelect();

        // Cannot push the predicate down onto the t quantifier because g is correlated to t

        testHelper.assertYields(join, newJoin);
    }

    @Test
    void partitionPredicatesWithExists() {
        // Partition a join where one of the quantifiers is over an existential quantifier. We can
        // partition it, which will result in us creating a select expression over the existential,
        // and then projecting 1 as the value

        final Quantifier t = baseT();
        final Quantifier g = Quantifier.existential(explodeField(t, "g").getRangesOver());

        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(t, "a"))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "hello")))
                .addPredicate(new ExistsPredicate(g.getAlias()))
                .build().buildSelect();

        // Push the predicate on t down to t. Push the existential predicate to be on top of g
        final Quantifier newT = forEach(selectWithPredicates(t,
                fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "hello"))));
        final Quantifier newG = forEach(new SelectExpression(RCV_OF_ONE, ImmutableList.of(g), ImmutableList.of(new ExistsPredicate(g.getAlias()))));
        final SelectExpression newJoin = join(newT, newG)
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }

    @Test
    void partitionSelectWhenResultValueComesFromRecordWithExplode() {
        // Partition a binary join when there is an explode, and the exploded value does not contribute to the
        // final result value. In this case, we will insert an RCV of one as the result value when
        // pushing any predicates down onto it

        final Quantifier t = baseT();
        final Quantifier g = explodeField(t, "g");
        final QueryPredicate joinPredicate = fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(g, "two")));
        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(t, "a"))
                .addPredicate(joinPredicate)
                .build().buildSelect();

        // Predicate gets pushed down onto the g field.
        final Quantifier newG = forEach(new SelectExpression(RCV_OF_ONE, ImmutableList.of(g), ImmutableList.of(joinPredicate)));
        final SelectExpression newJoin = join(t, newG)
                .addResultColumn(projectColumn(t, "a"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }

    @Test
    void partitionSelectWhenResultValueComesFromExplodedField() {
        // Partition a binary join when there is an explode, and the record from which the explode originates does
        // not participate in the result value. However, we don't replace its result value with SELECT 1 as we
        // need it for the result value.

        final Quantifier t = baseT();
        final Quantifier g = explodeField(t, "g");
        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(g, "one"))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "hello")))
                .addPredicate(fieldPredicate(g, "two", new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "world")))
                .build().buildSelect();

        final Quantifier newT = forEach(selectWithPredicates(t, fieldPredicate(t, "b", new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "hello"))));
        final Quantifier newG = forEach(selectWithPredicates(g, fieldPredicate(g, "two", new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "world"))));
        final SelectExpression newJoin = join(newT, newG)
                .addResultColumn(projectColumn(newG, "one"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }
}

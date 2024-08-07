/*
 * QueryPredicateTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.DefaultQueryPredicateRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.QueryPredicateWithCnfRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.QueryPredicateWithDnfRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate.not;
import static com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers.field;
import static com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers.rcv;
import static com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers.rcv2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link QueryPredicate} implementations.
 */
@SuppressWarnings("SimplifiableAssertion")
public class QueryPredicateTest {
    private Boolean evaluate(@Nonnull QueryPredicate predicate) {
        return evaluate(predicate, Bindings.EMPTY_BINDINGS);
    }

    private Boolean evaluate(@Nonnull QueryPredicate predicate, @Nonnull Bindings bindings) {
        return predicate.eval(null, EvaluationContext.forBindings(bindings));
    }

    private QueryPredicate and(@Nonnull QueryPredicate... predicates) {
        return AndPredicate.and(ImmutableList.copyOf(predicates));
    }

    private QueryPredicate or(@Nonnull QueryPredicate... predicates) {
        return OrPredicate.or(ImmutableList.copyOf(predicates));
    }

    private abstract static class TestPredicate extends AbstractQueryPredicate implements LeafQueryPredicate {

        public TestPredicate() {
            super(false);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return 0;
        }

        @Override
        public boolean equals(final Object other) {
            return semanticEquals(other, AliasMap.emptyMap());
        }

        @Override
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return this == other;
        }

        @Override
        public int hashCode() {
            return semanticHashCode();
        }

        @Override
        public int computeSemanticHashCode() {
            return LeafQueryPredicate.super.computeSemanticHashCode();
        }

        @Override
        public int hashCodeWithoutChildren() {
            return 31;
        }
    }

    private static final QueryPredicate TRUE = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
            return Boolean.TRUE;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }
    };

    private static final QueryPredicate FALSE = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
            return Boolean.FALSE;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }
    };

    private static final QueryPredicate NULL = new TestPredicate() {
        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
            return null;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }
    };

    @Test
    public void testAnd() {
        assertNull(evaluate(and(TRUE, NULL)));
        // Use equals here, because assertTrue/False would throw nullPointerException if and() returns null
        assertEquals(true, evaluate(and(TRUE, TRUE)));
        assertEquals(false, evaluate(and(TRUE, FALSE)));
        assertEquals(false, evaluate(and(NULL, FALSE)));
        assertNull(evaluate(and(NULL, TRUE)));
    }

    @Test
    public void testOr() {
        final ExpressionTestsProto.TestScalarFieldAccess val = ExpressionTestsProto.TestScalarFieldAccess.newBuilder().build();
        assertNull(evaluate(or(FALSE, NULL)));
        // Use equals here, because assertTrue/False would throw nullPointerException if or() returns null
        assertEquals(true, evaluate(or(FALSE, TRUE)));
        assertEquals(true, evaluate(or(TRUE, FALSE)));
        assertEquals(false, evaluate(or(FALSE, FALSE)));
        assertEquals(true, evaluate(or(NULL, TRUE)));
    }

    @Test
    public void testOrEquivalence() {
        final var rcv = rcv();

        final var a = field(rcv, "a");
        final var b = field(rcv, "b");
        final var c = field(rcv, "c");

        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "World"));
        final var p3 = new ValuePredicate(c, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Castro"));

        assertTrue(OrPredicate.or(p1, p2, p3).hashCode() == OrPredicate.or(p3, p2, p1).hashCode());
        assertTrue(OrPredicate.or(p1, p2, p3).equals(OrPredicate.or(p3, p2, p1)));

        assertTrue(OrPredicate.or(p1, p1, p2, p3).hashCode() == OrPredicate.or(p3, p3, p3, p2, p1).hashCode());
        assertTrue(OrPredicate.or(p1, p1, p2, p3).equals(OrPredicate.or(p3, p3, p3, p2, p1)));
        assertTrue(OrPredicate.or(p1, p3, p2).hashCode() == OrPredicate.or(p1, p2, p3).hashCode());
        assertTrue(OrPredicate.or(p1, p3, p2).equals(OrPredicate.or(p1, p2, p3)));
        assertTrue(OrPredicate.or(p3, p1, p2).hashCode() == OrPredicate.or(p2, p3, p1).hashCode());
        assertTrue(OrPredicate.or(p3, p1, p2).equals(OrPredicate.or(p2, p3, p1)));

        assertFalse(OrPredicate.or(p1, p2).equals(OrPredicate.or(p2, p3, p1)));
        assertFalse(OrPredicate.or(p1, p2, p3).equals(OrPredicate.or(p2, p2)));
    }

    @Test
    public void testAndEquivalence() {
        final var rcv = rcv();

        final var a = field(rcv, "a");
        final var b = field(rcv, "b");
        final var c = field(rcv, "c");

        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "World"));
        final var p3 = new ValuePredicate(c, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Castro"));

        assertTrue(AndPredicate.and(p1, p2, p3).hashCode() == AndPredicate.and(p3, p2, p1).hashCode());
        assertTrue(AndPredicate.and(p1, p2, p3).equals(AndPredicate.and(p3, p2, p1)));
        assertTrue(AndPredicate.and(p1, p1, p2, p3).hashCode() == AndPredicate.and(p3, p3, p3, p2, p1).hashCode());
        assertTrue(AndPredicate.and(p1, p1, p2, p3).equals(AndPredicate.and(p3, p3, p3, p2, p1)));
        assertTrue(AndPredicate.and(p1, p3, p2).hashCode() == AndPredicate.and(p1, p2, p3).hashCode());
        assertTrue(AndPredicate.and(p1, p3, p2).equals(AndPredicate.and(p1, p2, p3)));
        assertTrue(AndPredicate.and(p3, p1, p2).hashCode() == AndPredicate.and(p2, p3, p1).hashCode());
        assertTrue(AndPredicate.and(p3, p1, p2).equals(AndPredicate.and(p2, p3, p1)));

        assertFalse(AndPredicate.and(p1, p2).equals(AndPredicate.and(p2, p3, p1)));
        assertFalse(AndPredicate.and(p1, p2, p3).equals(AndPredicate.and(p2, p2)));
    }

    @Test
    public void testAndOrEquivalence() {
        final var rcv = rcv();

        final var a = field(rcv, "a");
        final var b = field(rcv, "b");
        final var c = field(rcv, "c");

        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "World"));
        final var p3 = new ValuePredicate(c, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Castro"));

        assertTrue(AndPredicate.and(p1, OrPredicate.or(p2, p3)).equals(AndPredicate.and(OrPredicate.or(p3, p2), p1)));
        assertTrue(AndPredicate.and(p1, OrPredicate.or(p2, p3, p3), p1).equals(AndPredicate.and(OrPredicate.or(p3, p2), p1)));

        assertFalse(AndPredicate.and(p1, OrPredicate.or(p2, p3)).equals(AndPredicate.and(OrPredicate.or(p3, p2, p1), p1)));
    }

    @Test
    public void testQueryPredicateIdentityLawOptimization() {
        final var rcv = rcv();
        final var a = field(rcv, "a");
        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ConstantPredicate(false);

        final var predicate = OrPredicate.or(p1, p2);

        final var result = Simplification.optimize(predicate,
                EvaluationContext.empty(),
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                DefaultQueryPredicateRuleSet.ofComputationRules());

        assertTrue(result.getLeft().equals(p1));
    }

    @Test
    public void testQueryPredicateNotPushDownOptimization() {
        final var rcv = rcv();
        final var a = field(rcv, "a");
        final var b = field(rcv, "b");
        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "World"));

        final var predicate = not(OrPredicate.or(p1, p2));

        final var result = Simplification.optimize(predicate,
                EvaluationContext.empty(),
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                DefaultQueryPredicateRuleSet.ofComputationRules());

        final var notP1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, "Hello"));
        final var notP2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, "World"));
        assertTrue(result.getLeft().equals(AndPredicate.and(notP1, notP2)));
    }

    @Test
    void cnfDnfRoundTrip() {
        final var rcv = rcv();
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv.getResultType());
        final var a = field(qov, "a");
        final var b = field(qov, "b");
        final var c = field(qov, "c");

        final var p1 = new ValuePredicate(a, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "Hello"));
        final var p2 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "World"));
        final var p22 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "World2"));
        final var p3 = new ValuePredicate(c, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "Something"));
        final var p32 = new ValuePredicate(b, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "Something2"));

        final var predicate = OrPredicate.or(p1, AndPredicate.and(p2, p22), AndPredicate.and(p3, p32));

        final var cnfPredicatePair = Simplification.optimize(predicate, EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(), QueryPredicateWithCnfRuleSet.ofComputationRules());
        final var dnfPredicatePair = Simplification.optimize(cnfPredicatePair.getLeft(), EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(), QueryPredicateWithDnfRuleSet.ofComputationRules());
        assertTrue(dnfPredicatePair.getLeft().equals(predicate));
    }

    @Test
    void simplifyPredicateTestRemovalRedundantSubPredicates() {
        final var rcv = rcv2();
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv.getResultType());
        final var name = field(qov, "name");
        final var rest_no = field(qov, "rest_no");
        // (c1 + c2 > rest_no AND c1 + c2 > rest_no) OR name = 'foo'
        final var c1 = ConstantObjectValue.of( Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.INT));
        final var c2 = ConstantObjectValue.of( Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.INT));
        final var restnoGtC1PlusC2 = new ValuePredicate(rest_no, new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN,
                (Value)new ArithmeticValue.AddFn().encapsulate(List.of(c1, c2))));
        final var nameEqFoo = new ValuePredicate(name, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar("foo")));
        final var predicate = or(and(restnoGtC1PlusC2, restnoGtC1PlusC2), nameEqFoo);
        final var expectedSimplifiedPredicate = or(restnoGtC1PlusC2, nameEqFoo);
        final var simplifiedPredicate =
                Simplification.optimize(predicate, EvaluationContext.empty(), AliasMap.emptyMap(),
                        ImmutableSet.of(), QueryPredicateWithDnfRuleSet.ofComputationRules()).getLeft();
        assertEquals(expectedSimplifiedPredicate, simplifiedPredicate);
    }

    @Test
    void simplifyPredicateTestRemovalRedundantDeepSubPredicates() {
        final var rcv = rcv2();
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv.getResultType());
        final var name = field(qov, "name");
        final var rest_no = field(qov, "rest_no");

        // c1 < rest_no AND ((c1 + c2 < rest_no AND c1 + c2 < rest_no) OR name = 'foo') AND c2 < rest_no
        final var c1 = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.INT));
        final var c2 = ConstantObjectValue.of(Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.INT));
        final var restnoGtC1PlusC2 = new ValuePredicate(rest_no, new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN,
                (Value)new ArithmeticValue.AddFn().encapsulate(List.of(c1, c2))));
        final var nameEqFoo = new ValuePredicate(name, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar("foo")));
        final var restnoGtC1 = new ValuePredicate(rest_no, new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, c1));
        final var restnoGtC2 = new ValuePredicate(rest_no, new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, c2));

        final var predicate = and(restnoGtC1, or(and(restnoGtC1PlusC2, restnoGtC1PlusC2), nameEqFoo), restnoGtC2);
        final var expectedSimplifiedPredicate = or(and(restnoGtC1, restnoGtC1PlusC2, restnoGtC2), and(restnoGtC1, nameEqFoo, restnoGtC2));

        final var simplifiedPredicate =
                Simplification.optimize(predicate, EvaluationContext.empty(), AliasMap.emptyMap(),
                        ImmutableSet.of(), QueryPredicateWithDnfRuleSet.ofComputationRules()).getLeft();
        assertEquals(expectedSimplifiedPredicate, simplifiedPredicate);
    }
}

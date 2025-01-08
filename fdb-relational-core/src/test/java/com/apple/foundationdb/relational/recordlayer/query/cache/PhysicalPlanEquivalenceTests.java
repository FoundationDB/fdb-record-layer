/*
 * PhysicalPlanEquivalenceTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.relational.recordlayer.query.QueryExecutionContext.OrderedLiteral.constantId;

/**
 * This contains a set of tests the verify equality semantics which is necessary for secondary cache load, lookup,
 * and eviction behavior.
 */
@API(API.Status.EXPERIMENTAL)
public class PhysicalPlanEquivalenceTests {

    @Nonnull
    private static final TypeRepository EMPTY_TYPE_REPO = TypeRepository.empty();

    @Nonnull
    private static final QueryPredicate lt00 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(),
                    constantId(0), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

    @Nonnull
    private static final QueryPredicate gt400 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(),
                    constantId(0), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 400));

    @Nonnull
    private static final QueryPredicate lt100Dup = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(),
                    constantId(0), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

    @Nonnull
    private static final QueryPredicate lt400 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(),
                    constantId(0), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 400));

    @Nonnull
    private static final QueryPlanConstraint trueConstraint = QueryPlanConstraint.tautology();

    @Nonnull
    private static final QueryPlanConstraint lt100Constraint = QueryPlanConstraint.ofPredicate(lt00);

    @Nonnull
    private static final QueryPlanConstraint lt100ConstraintDup = QueryPlanConstraint.ofPredicate(lt100Dup);

    @Nonnull
    private static final QueryPlanConstraint gt400Constraint = QueryPlanConstraint.ofPredicate(gt400);

    @Nonnull
    private static final QueryPlanConstraint lt400Constraint = QueryPlanConstraint.ofPredicate(lt400);

    @Nonnull
    private static final EvaluationContext ec80 = EvaluationContext.newBuilder()
            .setConstant(Quantifier.constant(), Map.of(constantId(0), 80)).build(EMPTY_TYPE_REPO);

    @Nonnull
    private static final EvaluationContext ec120 = EvaluationContext.newBuilder()
            .setConstant(Quantifier.constant(), Map.of(constantId(0), 120)).build(EMPTY_TYPE_REPO);

    @Nonnull
    private static final EvaluationContext ec120Dup = EvaluationContext.newBuilder()
            .setConstant(Quantifier.constant(), Map.of(constantId(0), 120)).build(EMPTY_TYPE_REPO);

    @Test
    void equalityOfPhysicalPlanEquivalenceWithMatchingConstraintsWorks() {
        // basic check for reference equality.
        final var ppe1 = PhysicalPlanEquivalence.of(trueConstraint);
        Assertions.assertThat(ppe1.equals(ppe1)).isTrue();

        // member-wise hashing / equality.
        final var ppe2 = PhysicalPlanEquivalence.of(lt100Constraint);
        final var ppe2Equal = PhysicalPlanEquivalence.of(lt100ConstraintDup);
        Assertions.assertThat(ppe2).isEqualTo(ppe2Equal);

        // negative test
        final var ppe3 = PhysicalPlanEquivalence.of(trueConstraint);
        Assertions.assertThat(ppe2).isNotEqualTo(ppe3);
    }

    @Test
    void equalityOfPhysicalPlanEquivalenceWithMatchingEvaluationContextsWorks() {
        // basic check for reference equality.
        final var ppe1 = PhysicalPlanEquivalence.of(ec80);
        Assertions.assertThat(ppe1.equals(ppe1)).isTrue();

        // member-wise hashing / equality.
        final var ppe2 = PhysicalPlanEquivalence.of(ec120);
        final var ppe2Equal = PhysicalPlanEquivalence.of(ec120Dup);
        Assertions.assertThat(ppe2).isEqualTo(ppe2Equal);

        // negative test
        final var ppe3 = PhysicalPlanEquivalence.of(ec80);
        Assertions.assertThat(ppe2).isNotEqualTo(ppe3);
    }

    @Test
    void lookupWithMatchingEvaluationContextWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                PhysicalPlanEquivalence.of(lt100Constraint), "Matching Plan",
                PhysicalPlanEquivalence.of(gt400Constraint), "Non Matching Plan");
        final var needle = PhysicalPlanEquivalence.of(ec80);

        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isEqualTo("Matching Plan");
    }

    @Test
    void lookupWithMultipleMatchingEvaluationContextsWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                PhysicalPlanEquivalence.of(lt100Constraint), "Matching Plan 1",
                PhysicalPlanEquivalence.of(lt400Constraint), "Matching Plan 2",
                PhysicalPlanEquivalence.of(gt400Constraint), "Non-matching Plan");
        final var needle = PhysicalPlanEquivalence.of(ec80);
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        final var expectedAnswers = Set.of("Matching Plan 1", "Matching Plan2");

        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(expectedAnswers).contains(found);
    }

    @Test
    void lookupWithNonMatchingEvaluationContextsWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                PhysicalPlanEquivalence.of(lt100Constraint), "Non-Matching Plan 1",
                PhysicalPlanEquivalence.of(gt400Constraint), "Non-Matching Plan 2");
        final var needle = PhysicalPlanEquivalence.of(ec120);
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isNull();
    }
}

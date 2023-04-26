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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This contains a set of tests the verify equality semantics which is necessary for secondary cache load, lookup,
 * and eviction behavior.
 */
public class PhysicalPlanEquivalenceTests {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static class PPETestBuilder {
        private int schemaTemplateVersion;

        private int userVersion;

        private Set<String> requiredIndexes;

        private Optional<QueryPlanConstraint> constraint;

        private Optional<EvaluationContext> evaluationContext;

        public PPETestBuilder() {
            this.schemaTemplateVersion = 42;
            this.userVersion = 43;
            this.requiredIndexes = Set.of("a", "b", "c");
            this.evaluationContext = Optional.empty();
            this.constraint = Optional.empty();
        }

        @Nonnull
        public PPETestBuilder withConstraint(@Nonnull final QueryPlanConstraint constraint) {
            this.constraint = Optional.of(constraint);
            this.evaluationContext = Optional.empty();
            return this;
        }

        @Nonnull
        public PPETestBuilder withEvaluationContext(@Nonnull final EvaluationContext evaluationContext) {
            this.constraint = Optional.empty();
            this.evaluationContext = Optional.of(evaluationContext);
            return this;
        }

        @Nonnull
        public PPETestBuilder withIndexes(@Nonnull final Set<String> indexes) {
            this.requiredIndexes = indexes;
            return this;
        }

        @Nonnull
        public PPETestBuilder withSchemaTemplateVerison(int schemaTemplateVersion) {
            this.schemaTemplateVersion = schemaTemplateVersion;
            return this;
        }

        @Nonnull
        public PPETestBuilder withUserVersion(int userVersion) {
            this.userVersion = userVersion;
            return this;
        }

        @Nonnull
        public PhysicalPlanEquivalence build() {
            if (constraint.isPresent()) {
                Assert.thatUnchecked(evaluationContext.isEmpty());
            } else if (evaluationContext.isEmpty())  {
                Assert.failUnchecked("neither constraint nor evaluation context is set.");
            }
            return new PhysicalPlanEquivalence(schemaTemplateVersion, userVersion, requiredIndexes, constraint, evaluationContext);
        }
    }

    @Nonnull
    private static final TypeRepository EMPTY_TYPE_REPO = TypeRepository.empty();

    @Nonnull
    private static final QueryPredicate lt00 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

    @Nonnull
    private static final QueryPredicate gt400 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 400));

    @Nonnull
    private static final QueryPredicate lt100Dup = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 100));

    @Nonnull
    private static final QueryPredicate lt400 = new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)),
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
    private static final EvaluationContext ec80 = EvaluationContext.newBuilder().setConstant(Quantifier.constant(), List.of(80)).build(EMPTY_TYPE_REPO);

    @Nonnull
    private static final EvaluationContext ec120 = EvaluationContext.newBuilder().setConstant(Quantifier.constant(), List.of(120)).build(EMPTY_TYPE_REPO);

    @Nonnull
    private static final EvaluationContext ec120Dup = EvaluationContext.newBuilder().setConstant(Quantifier.constant(), List.of(120)).build(EMPTY_TYPE_REPO);

    @Nonnull
    private static PPETestBuilder newPPETestBuilder() {
        return new PPETestBuilder();
    }

    @Test
    void equalityOfPPEWithMatchingConstraintsWorks() {
        // basic check for reference equality.
        final var ppe1 = newPPETestBuilder().withConstraint(trueConstraint).build();
        Assertions.assertThat(ppe1.equals(ppe1)).isTrue();

        // member-wise hashing / equality.
        final var ppe2 = newPPETestBuilder().withConstraint(lt100Constraint).build();
        final var ppe2Equal = newPPETestBuilder().withConstraint(lt100ConstraintDup).build();
        Assertions.assertThat(ppe2).isEqualTo(ppe2Equal);

        // negative test
        final var ppe3 = newPPETestBuilder().withConstraint(trueConstraint).build();
        Assertions.assertThat(ppe2).isNotEqualTo(ppe3);
    }

    @Test
    void equalityOfPPEWithMatchingEvaluationContextsWorks() {
        // basic check for reference equality.
        final var ppe1 = newPPETestBuilder().withEvaluationContext(ec80).build();
        Assertions.assertThat(ppe1.equals(ppe1)).isTrue();

        // member-wise hashing / equality.
        final var ppe2 = newPPETestBuilder().withEvaluationContext(ec120).build();
        final var ppe2Equal = newPPETestBuilder().withEvaluationContext(ec120Dup).build();
        Assertions.assertThat(ppe2).isEqualTo(ppe2Equal);

        // negative test
        final var ppe3 = newPPETestBuilder().withEvaluationContext(ec80).build();
        Assertions.assertThat(ppe2).isNotEqualTo(ppe3);
    }

    @Test
    void lookupWithMatchingEvaluationContextWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).build(), "Matching Plan",
                newPPETestBuilder().withConstraint(gt400Constraint).build(), "Non Matching Plan");
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).build();

        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isEqualTo("Matching Plan");
    }

    @Test
    void lookupWithMultipleMatchingEvaluationContextsWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).build(), "Matching Plan 1",
                newPPETestBuilder().withConstraint(lt400Constraint).build(), "Matching Plan 2",
                newPPETestBuilder().withConstraint(gt400Constraint).build(), "Non-matching Plan");
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).build();
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
                newPPETestBuilder().withConstraint(lt100Constraint).build(), "Non-Matching Plan 1",
                newPPETestBuilder().withConstraint(gt400Constraint).build(), "Non-Matching Plan 2");
        final var needle = newPPETestBuilder().withEvaluationContext(ec120).build();
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isNull();
    }

    @Test
    void lookupWithNonMatchingDueToEvolvedSchemaTemplateVersionWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).withSchemaTemplateVerison(100).build(), "Non-Matching Plan 1",
                newPPETestBuilder().withConstraint(gt400Constraint).withSchemaTemplateVerison(101).build(), "Non-Matching Plan 2");

        // simulate a plan with a much more recent schema template version.
        // notice that the evaluation context satisfies constraints in both plans otherwise.
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).withSchemaTemplateVerison(104).build();
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isNull();
    }

    @Test
    void lookupWithNonMatchingDueToEvolvedUserVersionWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).withUserVersion(100).build(), "Non-Matching Plan 1",
                newPPETestBuilder().withConstraint(gt400Constraint).withUserVersion(101).build(), "Non-Matching Plan 2");

        // simulate a plan with a much more recent user version.
        // notice that the evaluation context satisfies constraints in both plans otherwise.
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).withUserVersion(104).build();
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isNull();
    }

    @Test
    void lookupWithNonMatchingDueToMissingIndexesWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).withIndexes(Set.of("a", "b")).build(), "Non-Matching Plan 1",
                newPPETestBuilder().withConstraint(gt400Constraint).withIndexes(Set.of("b", "c")).build(), "Non-Matching Plan 2");

        // simulate a plan with readable indexes in the record store that misses some of the seen indexes in both cached plans
        // (index "a" in first cached plan, and index "b" in second cached plan).
        // notice that the evaluation context satisfies constraints in both plans, along with schema template version
        // and user version otherwise.
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).withIndexes(Set.of("c", "d")).build();
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(found).isNull();
    }

    @Test
    void lookupWithMatchingWithAllRequiredIndexesAndSomeExtraOnesWorks() {
        // assume we have the following entry in the cache
        // note that entries stored in the secondary cache _must_ not have an evaluation context, only a constraint.
        final var haystack = Map.of(
                newPPETestBuilder().withConstraint(lt100Constraint).withIndexes(Set.of("a", "b")).build(), "Matching Plan 1",
                newPPETestBuilder().withConstraint(gt400Constraint).withIndexes(Set.of("b", "c")).build(), "Matching Plan 2");
        final var expectedAnswers = Set.of("Matching Plan 1", "Matching Plan2");

        // simulate a plan with readable indexes in the record store that misses some of the seen indexes in both cached plans
        // (index "a" in first cached plan, and index "b" in second cached plan).
        // notice that the evaluation context satisfies constraints in both plans, along with schema template version
        // and user version otherwise.
        final var needle = newPPETestBuilder().withEvaluationContext(ec80).withIndexes(Set.of("a", "b", "c", "d")).build();
        // note that, in current behavior, we return _any_ matching plan for performance reasons.
        // simulate cache lookup.
        final var found = haystack.get(needle);
        Assertions.assertThat(expectedAnswers).contains(found);
    }
}

/*
 * ConstantFoldingTestUtils.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantFoldingRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class ConstantFoldingTestUtils {

    private static int counter;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class ValueWrapper {

        @Nonnull
        private final Optional<EvaluationContext> evaluationContext;

        @Nonnull
        private final Value value;


        private ValueWrapper(@Nonnull final Value value, @Nonnull final Optional<EvaluationContext> evaluationContext) {
            this.evaluationContext = evaluationContext;
            this.value = value;
        }

        @Nonnull
        public Value value() {
            return value;
        }

        @Nonnull
        public Optional<EvaluationContext> getEvaluationContextMaybe() {
            return evaluationContext;
        }

        @Nonnull
        public EvaluationContext getEvaluationContext() {
            Verify.verify(getEvaluationContextMaybe().isPresent());
            return getEvaluationContextMaybe().get();
        }

        @Nonnull
        public EvaluationContext getEvaluationContextOrEmpty() {
            return evaluationContext.orElse(EvaluationContext.EMPTY);
        }

        @Nonnull
        public ValueWrapper withNewValue(@Nonnull Value value) {
            return new ValueWrapper(value, evaluationContext);
        }

        @Override
        public String toString() {
            if (value() instanceof ConstantObjectValue) {
                Verify.verify(evaluationContext.isPresent());
                return "(CoV → " + value().evalWithoutStore(evaluationContext.get()) + ")";
            }
            if (value instanceof LiteralValue<?>) {
                return "(ℓ " + ((LiteralValue<?>)value).getLiteralValue() + ")";
            }
            if (value instanceof NullValue) {
                return "∅";
            }
            return value.toString();
        }

        @Nonnull
        public EvaluationContext mergeEvaluationContext(@Nonnull final ValueWrapper that) {
            if (evaluationContext.isEmpty() && that.evaluationContext.isEmpty()) {
                return EvaluationContext.empty();
            }
            if (evaluationContext.isEmpty()) {
                return that.evaluationContext.get();
            }
            if (that.evaluationContext.isEmpty()) {
                return evaluationContext.get();
            }
            final var thisBindingsChildBuilder = evaluationContext.get().getBindings().childBuilder();
            final var thatBindings = that.evaluationContext.get().getBindings();
            thatBindings.asMappingList().forEach(entry -> thisBindingsChildBuilder.set(entry.getKey(), entry.getValue()));
            return EvaluationContext.forBindings(thisBindingsChildBuilder.build());
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final EvaluationContext evaluationContext, @Nonnull final ConstantObjectValue constantObjectValue) {
            return new ValueWrapper(constantObjectValue, Optional.of(evaluationContext));
        }

        @Nonnull
        public static ValueWrapper of(@Nonnull final Value value) {
            return new ValueWrapper(value, Optional.empty());
        }
    }

    @Nonnull
    public static ValueWrapper newCov(@Nonnull final Type type, @Nullable Object bindingValue) {
        final var correlationId = CorrelationIdentifier.uniqueID();
        final var constantId = String.valueOf(counter++);
        final var bindingKey = Bindings.Internal.CONSTANT.bindingName(correlationId.getId());
        final var bindingValueMap = new HashMap<String, Object>();
        bindingValueMap.put(constantId, bindingValue);
        return ValueWrapper.of(EvaluationContext.forBinding(bindingKey, bindingValueMap), ConstantObjectValue.of(correlationId, constantId, type));
    }

    @Nonnull
    public static ValueWrapper litNull() {
        return ValueWrapper.of(new NullValue(Type.nullType()));
    }

    @Nonnull
    public static ValueWrapper covNull() {
        return newCov(Type.nullType(), null);
    }

    @Nonnull
    public static ValueWrapper litFalse() {
        return ValueWrapper.of(LiteralValue.ofScalar(false));
    }

    @Nonnull
    public static ValueWrapper covFalse() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    }

    @Nonnull
    public static ValueWrapper litTrue() {
        return ValueWrapper.of(LiteralValue.ofScalar(true));
    }

    @Nonnull
    public static ValueWrapper covTrue() {
        return newCov(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    }

    @Nonnull
    public static ValueWrapper litString(@Nonnull final String value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    @Nonnull
    public static ValueWrapper litInt(int value) {
        return ValueWrapper.of(LiteralValue.ofScalar(value));
    }

    @Nonnull
    public static ValueWrapper notNullIntCov() {
        return newCov(Type.primitiveType(Type.TypeCode.INT, false), 42);
    }

    @Nonnull
    public static ValueWrapper qov(Type type) {
        final QuantifiedObjectValue qov = QuantifiedObjectValue.of(Quantifier.current(), type);
        return new ValueWrapper(qov, Optional.empty());
    }

    @Nonnull
    public static ValueWrapper fieldValue(@Nonnull ValueWrapper baseValue, @Nonnull String fieldName) {
        final FieldValue fieldValue = FieldValue.ofFieldNameAndFuseIfPossible(baseValue.value(), fieldName);
        return baseValue.withNewValue(fieldValue);
    }

    @Nonnull
    public static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType) {
        return buildSingletonRange(comparisonType, null);
    }

    @Nonnull
    public static RangeConstraints buildSingletonRange(@Nonnull Comparisons.Type comparisonType, @Nullable final Value comparand) {
        final var singletonRangeBuilder = RangeConstraints.newBuilder();
        switch (comparisonType) {
            case IS_NULL: // fallthrough
            case NOT_NULL:
                singletonRangeBuilder.addComparisonMaybe(new Comparisons.NullComparison(comparisonType));
                break;
            default:
                singletonRangeBuilder.addComparisonMaybe(new Comparisons.ValueComparison(comparisonType, Verify.verifyNotNull(comparand)));
        }
        final var singletonRange = singletonRangeBuilder.build();
        Assertions.assertThat(singletonRange).isPresent();
        return singletonRange.get();
    }

    @Nonnull
    public static QueryPredicate isNotNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
    }

    @Nonnull
    public static QueryPredicate isNotNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.NOT_NULL)));
    }

    @Nonnull
    public static QueryPredicate isNull(@Nonnull final Value value) {
        return new ValuePredicate(value, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
    }

    @Nonnull
    public static QueryPredicate isNullAsRange(@Nonnull final Value value) {
        return PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.of(buildSingletonRange(Comparisons.Type.IS_NULL)));

    }

    @Nonnull
    public static QueryPredicate areEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, value2));
    }

    @Nonnull
    public static QueryPredicate areEqualAsRange(@Nonnull final Value value1, @Nonnull final Value value2) {
        return PredicateWithValueAndRanges.ofRanges(value1, ImmutableSet.of(buildSingletonRange(Comparisons.Type.EQUALS, value2)));

    }

    @Nonnull
    public static QueryPredicate areNotEqual(@Nonnull final Value value1, @Nonnull final Value value2) {
        return new ValuePredicate(value1, new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, value2));
    }

    @Nonnull
    public static QueryPredicate and(@Nonnull final QueryPredicate... preds) {
        return AndPredicate.and(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static QueryPredicate or(@Nonnull final QueryPredicate... preds) {
        return OrPredicate.or(Arrays.stream(preds).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static QueryPredicate simplify(@Nonnull final QueryPredicate predicate) {
        return simplify(predicate, EvaluationContext.empty());
    }

    @Nonnull
    public static QueryPredicate simplify(@Nonnull final QueryPredicate predicate,
                                          @Nonnull final EvaluationContext evaluationContext) {
        final var result = Simplification.optimize(predicate,
                evaluationContext,
                AliasMap.emptyMap(),
                ImmutableSet.of(),
                ConstantFoldingRuleSet.ofSimplificationRules());
        return result.get();
    }
}

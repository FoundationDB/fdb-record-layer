/*
 * InOpValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link Value} that checks if the left child is in the list of values.
 */
@API(API.Status.EXPERIMENTAL)
public class InOpValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("In-Op-Value");

    @Nonnull
    private final Value probeValue;
    @Nonnull
    private final Value inArrayValue;
    @Nonnull
    private final Function<Value, Object> compileTimeEvalFn;

    /**
     * Creates a new instance of {@link InOpValue}.
     * @param probeValue The left child in `IN` operator
     * @param inArrayValue The right child in `IN` operator
     * @param compileTimeEvalFn The compile-time function used to evaluate the expression.
     */
    private InOpValue(@Nonnull final Value probeValue,
                      @Nonnull final Value inArrayValue,
                      @Nonnull final Function<Value, Object> compileTimeEvalFn) {
        this.probeValue = probeValue;
        this.inArrayValue = inArrayValue;
        this.compileTimeEvalFn = compileTimeEvalFn;
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        final var builder = new ImmutableList.Builder<Value>();
        builder.add(probeValue);
        builder.add(inArrayValue);
        return builder.build();
    }

    @Nonnull
    @Override
    public InOpValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new InOpValue(Iterables.get(newChildren, 0), Iterables.get(newChildren, 1), compileTimeEvalFn);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var probeResult = probeValue.eval(store, context);
        final var inArrayResult = inArrayValue.eval(store, context);
        Verify.verify(inArrayResult instanceof List<?>);
        if (((List<?>) inArrayResult).stream().anyMatch(object -> object != null && object.equals(probeResult))) {
            return true;
        } else if (((List<?>) inArrayResult).stream().anyMatch(Objects::isNull)) {
            return null;
        } else {
            return false;
        }
    }

    @SuppressWarnings("java:S3776")
    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nonnull final CorrelationIdentifier innermostAlias) {
        final var leftChildCorrelatedTo = probeValue.getCorrelatedTo();

        Verify.verify(inArrayValue instanceof AbstractArrayConstructorValue.LightArrayConstructorValue);
        final var arrayConstructorList = (AbstractArrayConstructorValue.LightArrayConstructorValue) inArrayValue;
        final var isLiteralList = arrayConstructorList.getChildren().stream()
                .allMatch(value -> value.getCorrelatedTo()
                        .isEmpty());
        SemanticException.check(isLiteralList, SemanticException.ErrorCode.UNSUPPORTED);

        if (leftChildCorrelatedTo.isEmpty()) {
            return compileTimeEvalMaybe();
        }
        final var literalValue = compileTimeEvalFn.apply(inArrayValue);
        return Optional.of(new ValuePredicate(probeValue, new Comparisons.ListComparison(Comparisons.Type.IN, (List<?>) literalValue)));
    }

    private Optional<QueryPredicate> compileTimeEvalMaybe() {
        Object constantValue = compileTimeEvalFn.apply(this);
        if (constantValue instanceof Boolean) {
            if ((boolean) constantValue) {
                return Optional.of(ConstantPredicate.TRUE);
            } else {
                return Optional.of(ConstantPredicate.FALSE);
            }
        } else if (constantValue == null) {
            return Optional.of(ConstantPredicate.NULL);
        }
        return Optional.empty();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, probeValue, inArrayValue);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" + probeValue.explain(formatter) + " IN " + inArrayValue.explain(formatter) + ")";
    }

    @Override
    public String toString() {
        return "(" + probeValue + " IN " + inArrayValue + ")";
    }


    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    /**
     * The {@code in} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class InFn extends BuiltInFunction<Value> {
        public InFn() {
            super("in",
                    List.of(new Type.Any(), new Type.Array()), (ctx, builtInFunc, args) -> encapsulateInternal(ctx, args));
        }

        @Nonnull
        private static Value encapsulateInternal(@Nonnull TypeRepository.Builder typeRepositoryBuilder, @Nonnull final List<Typed> arguments) {
            final Typed arg0 = arguments.get(0);
            final Type res0 = arg0.getResultType();
            SemanticException.check(res0.isPrimitive(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);

            final Typed arg1 = arguments.get(1);
            final Type res1 = arg1.getResultType();
            SemanticException.check(res1.getTypeCode() == Type.TypeCode.ARRAY, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

            final var arrayElementType = ((Type.Array) res1).getElementType();
            if (res0.getTypeCode() != arrayElementType.getTypeCode()) {
                final var maximumType = Type.maximumType(arg0.getResultType(), arrayElementType);

                // Incompatible types
                SemanticException.check(maximumType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

                // Only promote if the resultant type is different
                if (!arg0.getResultType().equals(maximumType)) {
                    return new InOpValue(PromoteValue.inject((Value)arg0, maximumType), (Value) arg1, value -> value.compileTimeEval(EvaluationContext.forTypeRepository(typeRepositoryBuilder.build())));
                }

                // Do not currently promote the elements of the array
                SemanticException.check(maximumType == arrayElementType, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
            return new InOpValue((Value)arg0, (Value)arg1, value -> value.compileTimeEval(EvaluationContext.forTypeRepository(typeRepositoryBuilder.build())));
        }
    }
}

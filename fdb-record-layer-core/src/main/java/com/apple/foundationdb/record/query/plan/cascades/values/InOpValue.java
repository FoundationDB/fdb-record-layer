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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInOpValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link Value} that checks if the left child is in the list of values.
 */
@API(API.Status.EXPERIMENTAL)
public class InOpValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("In-Op-Value");

    @Nonnull
    private final Value probeValue;
    @Nonnull
    private final Value inArrayValue;

    /**
     * Creates a new instance of {@link InOpValue}.
     * @param probeValue The left child in `IN` operator
     * @param inArrayValue The right child in `IN` operator
     */
    private InOpValue(@Nonnull final Value probeValue,
                      @Nonnull final Value inArrayValue) {
        this.probeValue = probeValue;
        this.inArrayValue = inArrayValue;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(probeValue, inArrayValue);
    }

    @Nonnull
    @Override
    public InOpValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new InOpValue(Iterables.get(newChildren, 0), Iterables.get(newChildren, 1));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var probeResult = probeValue.eval(store, context);
        final var inArrayResult = inArrayValue.eval(store, context);
        Verify.verify(inArrayResult instanceof List<?>);
        if (((List<?>) inArrayResult).stream().anyMatch(object -> Boolean.TRUE.equals(Comparisons.evalComparison(Comparisons.Type.EQUALS, object, probeResult)))) {
            return true;
        } else if (((List<?>) inArrayResult).stream().anyMatch(Objects::isNull)) {
            return null;
        } else {
            return false;
        }
    }

    @SuppressWarnings("java:S3776")
    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable TypeRepository typeRepository, @Nonnull final CorrelationIdentifier innermostAlias) {
        // we fail if the right side is not evaluable as we cannot create the comparison

        final var leftChildCorrelatedTo = probeValue.getCorrelatedTo();
        if (leftChildCorrelatedTo.isEmpty() && typeRepository != null) {
            return compileTimeEvalMaybe(typeRepository);
        }

        final var isLiteralList = inArrayValue.getCorrelatedTo().isEmpty();
        SemanticException.check(isLiteralList, SemanticException.ErrorCode.UNSUPPORTED);

        if (typeRepository != null) {
            final var literalValue = Preconditions.checkNotNull(inArrayValue.compileTimeEval(EvaluationContext.forTypeRepository(typeRepository)));
            return Optional.of(new ValuePredicate(probeValue, new Comparisons.ListComparison(Comparisons.Type.IN, (List<?>)literalValue)));
        } else {
            return Optional.of(new ValuePredicate(probeValue, new Comparisons.ValueComparison(Comparisons.Type.IN, inArrayValue)));
        }
    }

    @Nonnull
    @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    private Optional<QueryPredicate> compileTimeEvalMaybe(@Nonnull TypeRepository typeRepository) {
        Object constantValue = this.compileTimeEval(EvaluationContext.forTypeRepository(typeRepository));
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
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, probeValue, inArrayValue);
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
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PInOpValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInOpValue.newBuilder()
                .setProbeValue(probeValue.toValueProto(serializationContext))
                .setInArrayValue(inArrayValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setInOpValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static InOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PInOpValue inOpValueProto) {
        return new InOpValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(inOpValueProto.getProbeValue())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(inOpValueProto.getInArrayValue())));
    }

    /**
     * The {@code in} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class InFn extends BuiltInFunction<Value> {
        public InFn() {
            super("in",
                    List.of(new Type.Any(), new Type.Array()), (builtInFunc, args) -> encapsulateInternal(args));
        }

        @Nonnull
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        private static Value encapsulateInternal(@Nonnull final List<? extends Typed> arguments) {
            final Typed arg0 = arguments.get(0);
            final Type res0 = arg0.getResultType();

            final Typed arg1 = arguments.get(1);
            final Type res1 = arg1.getResultType();
            SemanticException.check(res1.isArray(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

            final var arrayElementType = Objects.requireNonNull(((Type.Array) res1).getElementType());
            if (!arrayElementType.isUnresolved() && res0.getTypeCode() != arrayElementType.getTypeCode()) {
                final var maximumType = Type.maximumType(arg0.getResultType(), arrayElementType);
                // Incompatible types
                SemanticException.check(maximumType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);

                // Promote arg0 if the resulting type is different
                if (!arg0.getResultType().equals(maximumType)) {
                    return new InOpValue(PromoteValue.inject((Value)arg0, maximumType), (Value)arg1);
                } else {
                    return new InOpValue((Value)arg0, PromoteValue.inject((Value)arg1, new Type.Array(maximumType)));
                }
            }

            if (res0.isRecord()) {
                // we cannot yet promote this properly
                SemanticException.check(arrayElementType.isRecord(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
                final var probeElementTypes = Objects.requireNonNull(((Type.Record)res0).getElementTypes());
                final var inElementTypes = Objects.requireNonNull(((Type.Record)arrayElementType).getElementTypes());
                for (int i = 0; i < inElementTypes.size(); i++) {
                    final var probeElementType = probeElementTypes.get(i);
                    final var inElementType = inElementTypes.get(i);
                    SemanticException.check(probeElementType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
                    SemanticException.check(inElementType.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
                    SemanticException.check(probeElementType.getTypeCode() == inElementType.getTypeCode(),
                            SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
                }
            }
            return new InOpValue((Value)arg0, (Value)arg1);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PInOpValue, InOpValue> {
        @Nonnull
        @Override
        public Class<PInOpValue> getProtoMessageClass() {
            return PInOpValue.class;
        }

        @Nonnull
        @Override
        public InOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PInOpValue inOpValueProto) {
            return InOpValue.fromProto(serializationContext, inOpValueProto);
        }
    }
}

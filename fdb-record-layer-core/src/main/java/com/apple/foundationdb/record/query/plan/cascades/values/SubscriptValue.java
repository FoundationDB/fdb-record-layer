/*
 * SubscriptValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PSubscriptValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

public class SubscriptValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Subscript-Value");

    @Nonnull
    private final Value indexValue;

    @Nonnull
    private final Value sourceValue;

    @Nonnull
    private final Type type;

    public SubscriptValue(@Nonnull final Value indexValue,
                          @Nonnull final Value sourceValue) {
        this.indexValue = indexValue;
        this.sourceValue = sourceValue;
        this.type = Verify.verifyNotNull(((Type.Array)sourceValue.getResultType()).getElementType()).nullable();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(indexValue, sourceValue);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var indexValueExplain = Iterables.get(explainSuppliers, 0).get();
        final var sourceValueExplain = Iterables.get(explainSuppliers, 1).get();
        return ExplainTokensWithPrecedence.of(sourceValueExplain.getExplainTokens()
                .addOpeningSquareBracket().addOptionalWhitespace()
                .addNested(indexValueExplain.getExplainTokens())
                .addClosingAngledBracket());
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var index = indexValue.eval(store, context);
        if (index == null) {
            return null;
        }
        final var source = sourceValue.eval(store, context);
        if (source == null) {
            return null;
        }
        // index is 1-based as defined in SQL standard (Foundation, Section: 4.10.2):
        // > An array is a collection A in which each element is associated with exactly one ordinal position in A.
        //   If n is the cardinality of A, then the ordinal position p of an element is an integer in the range 1 (one)
        //   ≤ p ≤ n.
        final var sourceAsList = (List<?>)source;
        final var adjustedIndex = (int)index - 1;
        if (adjustedIndex < 0 || adjustedIndex >= sourceAsList.size()) {
            // this does not raise out-of-bound error.
            return null;
        }
        return sourceAsList.get(adjustedIndex);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return type;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder()
                .setSubscriptValue(toProto(serializationContext))
                .build();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH);
    }

    @Nonnull
    @Override
    public PSubscriptValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PSubscriptValue.newBuilder()
                .setIndex(indexValue.toValueProto(serializationContext))
                .setSource(sourceValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    public static SubscriptValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PSubscriptValue subscriptValueProto) {
        return new SubscriptValue(Value.fromValueProto(serializationContext, subscriptValueProto.getIndex()),
                Value.fromValueProto(serializationContext, subscriptValueProto.getSource()));
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        final var newIndexValue = Iterables.get(newChildren, 0);
        final var newSourceValue = Iterables.get(newChildren, 1);
        if (indexValue == newIndexValue && newSourceValue == sourceValue) {
            return this;
        }
        return new SubscriptValue(newIndexValue, newSourceValue);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PSubscriptValue, SubscriptValue> {
        @Nonnull
        @Override
        public Class<PSubscriptValue> getProtoMessageClass() {
            return PSubscriptValue.class;
        }

        @Nonnull
        @Override
        public SubscriptValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PSubscriptValue subscriptValueProto) {
            return SubscriptValue.fromProto(serializationContext, subscriptValueProto);
        }
    }

    /**
     * The {@code subscript} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class SubscriptValueFn extends BuiltInFunction<Value> {
        public SubscriptValueFn() {
            super("subscript", List.of(Type.primitiveType(Type.TypeCode.INT), Type.any()), Type.any(), SubscriptValue.SubscriptValueFn::encapsulate);
        }

        @SuppressWarnings({"PMD.UnusedFormalParameter", "PMD.UnusedPrivateMethod"}) // false positive, method is used
        private static Value encapsulate(@Nonnull BuiltInFunction<Value> ignored,
                                         @Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 2);
            var indexValue = (Value)arguments.get(0);
            final var indexMaxType = Type.maximumType(indexValue.getResultType(), Type.primitiveType(Type.TypeCode.INT));
            SemanticException.check(indexMaxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            indexValue = PromoteValue.inject(indexValue, indexMaxType);

            var sourceValue = (Value)arguments.get(1);
            Verify.verify(sourceValue.getResultType().isArray());

            return new SubscriptValue(indexValue, sourceValue);
        }
    }
}

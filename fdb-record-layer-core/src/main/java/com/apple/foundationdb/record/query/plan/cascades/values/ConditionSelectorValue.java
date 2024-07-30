/*
 * ConditionSelectorValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PConditionSelectorValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * This returns the index of the first implication that is satisfied.
 *
 * @see PickValue for more information.
 */
public class ConditionSelectorValue extends AbstractValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Condition-Selector-Value");

    @Nonnull
    private final List<? extends Value> implications;

    public ConditionSelectorValue(@Nonnull final Iterable<? extends Value> implications) {
        this.implications = ImmutableList.copyOf(implications);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return implications;
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        return new ConditionSelectorValue(newChildren);
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return implications.stream()
                .allMatch(implication -> implication.isFunctionallyDependentOn(otherValue));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.INT);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        for (int i = 0; i < implications.size(); ++i) {
            final var result = (Boolean)implications.get(i).eval(store, context);
            if (Boolean.TRUE.equals(result)) {
                return i;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ConditionSelector";
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, implications);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Nonnull
    @Override
    public PConditionSelectorValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PConditionSelectorValue.newBuilder();
        for (final Value implication : implications) {
            builder.addImplications(implication.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setConditionSelectorValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ConditionSelectorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PConditionSelectorValue conditionSelectorValueProto) {
        final ImmutableList.Builder<Value> implicationsBuilder = ImmutableList.builder();
        for (int i = 0; i < conditionSelectorValueProto.getImplicationsCount(); i ++) {
            implicationsBuilder.add(Value.fromValueProto(serializationContext, conditionSelectorValueProto.getImplications(i)));
        }
        return new ConditionSelectorValue(implicationsBuilder.build());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PConditionSelectorValue, ConditionSelectorValue> {
        @Nonnull
        @Override
        public Class<PConditionSelectorValue> getProtoMessageClass() {
            return PConditionSelectorValue.class;
        }

        @Nonnull
        @Override
        public ConditionSelectorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PConditionSelectorValue conditionSelectorValueProto) {
            return ConditionSelectorValue.fromProto(serializationContext, conditionSelectorValueProto);
        }
    }
}

/*
 * InstanceOfValue.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Checks whether a {@link Value}'s evaluation conforms to its result type.
 */
public class OfValueType implements Value, Value.CompileTimeValue, ValueWithChild {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Of-Value-Type");

    @Nonnull
    private final Value child;

    private final Type expectedType;

    public OfValueType(@Nonnull final Value child, @Nonnull final Type expectedType) {
        this.child = child;
        this.expectedType = expectedType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, child);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new OfValueType(rebasedChild);
    }

    @Nullable
    @Override
    public Object compileTimeEval(@Nonnull final EvaluationContext context) {
        return eval(null, context);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final var value = child.eval(store, context);
        final var typeCode = LiteralValue.typeCodeFromLiteral(value);
        final Type type;
        if (typeCode == null) {
            // value is null
            if (expectedType.isNullable()) {
                return true;
            } else {
                return false;
            }
        } if (typeCode == Type.TypeCode.ARRAY) {
            // special stuff for arrays
        } else {
            type = Type.primitiveType(typeCode);
        }

        if (expectedType.equals(type)) {
            //yay
        }

        final var javaClass = child.getResultType().getTypeCode().getJavaClass();
        if (javaClass == null) {
            return false;
        }
        return child.getResultType().getTypeCode().getJavaClass().isInstance(value);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public String toString() {
        return child + " ofType " + child.getResultType();
    }
}

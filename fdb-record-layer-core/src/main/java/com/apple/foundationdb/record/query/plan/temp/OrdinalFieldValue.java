/*
 * OrdinalFieldValue.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueWithChild;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * An {@link OrdinalFieldValue} accesses the n-th field of a tuple-{@link Type}'d child.
 *
 * For example:
 *   if the child <code>c</code> has a tuple type comprising three fields <code>{f[0]:String, f[1]:String, f[2]:Float}</code>,
 *   and we define <code>o = OrdinalFieldValue(c,1)</code>, then calling <code>c.eval</code> on this message
 *   <code>{'foo', 'bar', 3.14}</code> returns <code>'bar'</code>.
 */
@API(API.Status.EXPERIMENTAL)
public class OrdinalFieldValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Ordinal-Field-Value");

    @Nonnull
    private final Value child;
    private final int ordinalPosition;

    private final Type.Record.Field field;

    private OrdinalFieldValue(@Nonnull final Value child,
                              final int ordinalPosition) {
        SemanticException.check(child.getResultType().getTypeCode() == Type.TypeCode.RECORD, "child has to be a record");
        this.child = child;
        this.ordinalPosition = ordinalPosition;
        this.field = Objects.requireNonNull(((Type.Record)child.getResultType()).getFields()).get(ordinalPosition);
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return new OrdinalFieldValue(rebasedChild, ordinalPosition);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return field.getFieldType();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return child.explain(formatter) + "#" + ordinalPosition;
    }

    @SuppressWarnings("ConstantConditions")
    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> fdbRecord, @Nullable final M message) {
        final Message childTuple = (Message)child.eval(store, context, fdbRecord, message);
        final Descriptors.Descriptor descriptorForType = childTuple.getDescriptorForType();
        final Descriptors.FieldDescriptor fieldDescriptor = descriptorForType.findFieldByNumber(field.getFieldIndex());
        return childTuple.getField(fieldDescriptor);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!ValueWithChild.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final OrdinalFieldValue that = (OrdinalFieldValue)other;
        return ordinalPosition == that.getOrdinalPosition();
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, ordinalPosition);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, ordinalPosition);
    }

    @Override
    public String toString() {
        return child + "#" + ordinalPosition;
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(child.getCorrelatedTo()));
    }

    @Nonnull
    public static OrdinalFieldValue of(@Nonnull Value child, int ordinal) {
        return new OrdinalFieldValue(child, ordinal);
    }
}

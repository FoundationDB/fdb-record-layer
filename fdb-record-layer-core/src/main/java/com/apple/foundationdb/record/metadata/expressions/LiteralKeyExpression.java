/*
 * LiteralKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Expression to allow a static value to be utilized in a key expression.  This primary use case for this
 * is for passing static arguments to functions (see {@link FunctionKeyExpression} for details).
 * @param <T> the type of the literal value
 */
@API(API.Status.MAINTAINED)
public class LiteralKeyExpression<T> extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithValue, KeyExpressionWithoutChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Literal-Key-Expression");

    @Nullable
    private final T value;
    @Nonnull
    private final List<Key.Evaluated> evaluated;
    @Nonnull
    private final RecordMetaDataProto.Value proto;

    public LiteralKeyExpression(@Nullable T value) {
        // getProto() performs validation that it is a type we can serialize
        this(value, toProtoValue(value));
    }

    private LiteralKeyExpression(@Nullable T value, @Nonnull RecordMetaDataProto.Value proto) {
        this.value = value;
        this.evaluated = ImmutableList.of(value == null ? Key.Evaluated.NULL : Key.Evaluated.scalar(value));
        this.proto = proto;
    }

    @Nullable
    public T getValue() {
        return value;
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return evaluated;
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return Collections.emptyList();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Value toProto() throws SerializationException {
        return proto;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final CorrelationIdentifier baseAlias,
                         @Nonnull final Type baseType,
                         @Nonnull final List<String> fieldNamePrefix) {
        return LiteralValue.ofScalar(value);
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setValue(toProto()).build();
    }

    @Override
    public boolean needsCopyingToPartialRecord() {
        return false;
    }

    @Nonnull
    public static LiteralKeyExpression<?> fromProto(RecordMetaDataProto.Value proto) {
        return new LiteralKeyExpression<>(fromProtoValue(proto), proto);
    }

    @Nullable
    public static Object fromProtoValue(RecordMetaDataProto.Value proto) {
        int found = 0;
        Object value = null;
        if (proto.hasDoubleValue()) {
            ++found;
            value = proto.getDoubleValue();
        }
        if (proto.hasFloatValue()) {
            ++found;
            value = proto.getFloatValue();
        }
        if (proto.hasLongValue()) {                   
            ++found;
            value = proto.getLongValue();
        }
        if (proto.hasBoolValue()) {
            ++found;
            value = proto.getBoolValue();
        }
        if (proto.hasStringValue()) {
            ++found;
            value = proto.getStringValue();
        }
        if (proto.hasBytesValue()) {
            ++found;
            value = proto.getBytesValue().toByteArray();
        }
        if (proto.hasIntValue()) {
            ++found;
            value = proto.getIntValue();
        }
        if (found == 0) {
            ++found;
        }
        if (found > 1) {
            throw new RecordCoreException("More than one value encoded in value")
                    .addLogInfo("encoded_value", proto);
        }
        return value;
    }

    @Nonnull
    public static RecordMetaDataProto.Value toProtoValue(@Nullable Object value) {
        RecordMetaDataProto.Value.Builder builder = RecordMetaDataProto.Value.newBuilder();
        if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
        } else if (value instanceof Float) {
            builder.setFloatValue((Float)value);
        } else if (value instanceof Integer) {
            builder.setIntValue((Integer)value);
        } else if (value instanceof Number) {
            builder.setLongValue(((Number) value).longValue());
        } else if (value instanceof Boolean) {
            builder.setBoolValue((Boolean) value);
        } else if (value instanceof String) {
            builder.setStringValue((String) value);
        } else if (value instanceof byte[]) {
            builder.setBytesValue(ZeroCopyByteString.wrap((byte[])value));
        } else if (value != null) {
            throw new RecordCoreException("Unsupported value type").addLogInfo(
                    "value_type", value.getClass().getName());
        }
        return builder.build();
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LiteralKeyExpression)) {
            return false;
        }

        LiteralKeyExpression<?> other = (LiteralKeyExpression<?>) o;
        return proto.equals(other.proto);
    }

    @Override
    public int hashCode() {
        return proto.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, value);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        switch (hashKind) {
            case STRUCTURAL_WITH_LITERALS:
                return HashUtils.queryHash(hashKind, BASE_HASH, value);
            case STRUCTURAL_WITHOUT_LITERALS:
                return HashUtils.queryHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("value(");
        if (value instanceof String) {
            sb.append('"').append(value).append('"');
        } else {
            sb.append(value);
        }
        sb.append(')');
        return sb.toString();
    }
}

/*
 * FieldKeyExpression.java
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
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@code field()} key expression.
 *
 * <p>The {@code field()} expression takes keys from a record field specified by name. Besides the field name, it
 * carries two properties that control the behavior for {@code optional} or {@code repeated} fields:
 * <ul>
 * <li>The fan type ({@link com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType FanType}).</li>
 * <li>The null standin ({@link Key.Evaluated.NullStandin NullStandin}).</li>
 * </ul>
 *
 * <p>If the field is singular and {@code required}, the fan type is irrelevant and must be set to {@code None}.
 * In this case, {@code field()} always yields a single {@link Key.Evaluated} holding the scalar value.
 *
 * <p>If the field is singular and marked as {@code optional}, the fan type must be set to {@code None} (like for
 * {@code required} fields), and the {@code NullStandin} property drives the behavior in the “absent” case where the
 * field is not set on the processed message:
 * <ul>
 * <li>For {@code NULL} and {@code NULL_UNIQUE}, the {@code field()} expression yields a single {@code Key.Evaluated}
 *     carrying the standin, which represents an indexable null value in this case.
 * <li>For {@code NOT_NULL}, it substitutes the default value of the Protobuf type, such as 0 or {@code ""}.
 * </ul>
 *
 * <p>If the field is {@code repeated}, the fan type comes into play and must be set to either {@code FanOut} or
 * {@code Concatenate}:
 * <ul>
 * <li>For {@code FanOut}, the {@code field()} expression yields one {@code Key.Evaluated} per element of the repeated
 *     field.
 * <li>For {@code Concatenate}, it yields a single {@code Key.Evaluated} holding the entire repeated field as a
 *     {@link java.util.List List} object.
 * </ul>
 * Note that a {@code repeated} field cannot be “absent” in the sense of {@code optional}. If the {@code repeated} field
 * has 0 repetitions, {@code FanOut} will yield nothing while {@code Concatenate} will yield a single key holding an
 * empty list (since the empty list is the proto default for a repeated field).
 *
 * <p>A {@code field()} expression may also be evaluated on a message that itself is {@code null}. In this case, the
 * result depending on the fan type and the null standin is as follows:
 * <ul>
 * <li>For a singular field and fan type {@code None}, the {@code field()} expression yields a single key carrying the
 *     {@code NullStandin}.
 *     (TODO <a href="https://github.com/FoundationDB/fdb-record-layer/issues/4141/">Issue [#4141](#4141)</a>: NOT_NULL is currently returned as-is in this case instead of the proto default value.)
 * <li>For a repeated field and fan type {@code FanOut}, it yields no entries.
 * <li>For a repeated field and fan type {@code Concatenate}, if the standin is {@code NOT_NULL}, it behaves “as if 0
 *     repetitions” and yields a single entry holding the empty list. Otherwise, it behaves like {@code None} and emits
 *     a single key carrying the null standin.
 * </ul>
 * One common scenario where the message will be {@code null} is when the {@code field()} expression is the child
 * of a {@link NestingKeyExpression nest()} key expression, and the parent field evaluates to null.
 * For example, in the expression {@code field("array1").nest(field("values", Concatenate))}, if {@code array1} is an
 * {@code optional} field that is absent, then the outer {@code field("array1")} will yield its null standin and
 * {@code nest()} will funnel that as a {@code null} message to the inner {@code field("values", …)} expression,
 * which will in turn yield its null standin.
 */
@API(API.Status.UNSTABLE)
public class FieldKeyExpression extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithoutChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Key-Expression");

    /**
     * The internal field name used in the underlying protobuf message. This name may differ from the user-visible
     * identifier to ensure compliance with protobuf field naming conventions. When working with query planning or
     * user-facing operations, use {@link ProtoUtils#toUserIdentifier(String)} to convert this to the user-visible
     * form. However, when constructing physical operators that directly interact with stored protobuf messages,
     * this internal name should be used as-is.
     */
    @Nonnull
    private final String fieldName;
    @Nonnull
    private final FanType fanType;
    @Nonnull
    private final Key.Evaluated.NullStandin nullStandin;

    public FieldKeyExpression(@Nonnull String fieldName, @Nonnull FanType fanType, @Nonnull Key.Evaluated.NullStandin nullStandin) {
        this.fieldName = fieldName;
        this.fanType = fanType;
        this.nullStandin = nullStandin;
    }

    public FieldKeyExpression(@Nonnull RecordKeyExpressionProto.Field field) throws DeserializationException {
        if (!field.hasFieldName()) {
            throw new DeserializationException("Serialized Field is missing field name");
        }
        if (!field.hasFanType()) {
            throw new DeserializationException("Serialized Field is missing fan type");
        }
        fieldName = field.getFieldName();
        fanType = FanType.valueOf(field.getFanType());
        nullStandin = Key.Evaluated.NullStandin.valueOf(field.getNullInterpretation());
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor parentDescriptor) {
        return validate(parentDescriptor, false);
    }

    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor parentDescriptor, boolean allowMessageType) {
        final Descriptors.FieldDescriptor fieldDescriptor = parentDescriptor.findFieldByName(fieldName);
        validate(parentDescriptor, fieldDescriptor, allowMessageType);
        return Collections.singletonList(fieldDescriptor);
    }

    public void validate(@Nonnull Descriptors.Descriptor parentDescriptor, Descriptors.FieldDescriptor fieldDescriptor, boolean allowMessageType) {
        if (fieldDescriptor == null) {
            throw new InvalidExpressionException("Descriptor " + parentDescriptor.getName() + " does not have field: " + fieldName);
        }
        switch (fanType) {
            case FanOut:
            case Concatenate:
                if (!fieldDescriptor.isRepeated()) {
                    throw new InvalidExpressionException(
                            fieldName + " is not repeated with FanType." + fanType);
                }
                break;
            case None:
                if (fieldDescriptor.isRepeated()) {
                    throw new InvalidExpressionException(
                            fieldName + " is repeated with FanType.None");
                }
                break;
            default:
                throw new InvalidExpressionException("Unexpected FanType." + fanType);
        }
        if (!allowMessageType) {
            if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                    !TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                throw new Query.InvalidExpressionException(
                        fieldName + " is a nested message, but accessed as a scalar");
            }
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null) {
            return getNullResult();
        }
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(fieldName);
        // TODO: Part of this is working around a deficiency in DynamicMessage.getField() prior
        //  to 2.5, where a repeated message field returns an empty message instead of an
        //  empty collection.
        if (fieldDescriptor != null && fieldDescriptor.isRepeated()) {
            List<Object> values;
            if (message.getRepeatedFieldCount(fieldDescriptor) > 0) {
                values = (List<Object>)message.getField(fieldDescriptor);
            } else {
                values = Collections.emptyList();
            }

            switch (fanType) {
                case FanOut:
                    return Key.Evaluated.fan(values);
                case Concatenate:
                    return Collections.singletonList(Key.Evaluated.scalar(values));
                case None:
                    throw new RecordCoreException("FanType.None with repeated field");
                default:
                    throw new RecordCoreException("unknown fan type").addLogInfo(LogMessageKeys.VALUE, fanType);
            }
        } else if (fieldDescriptor != null && (nullStandin == Key.Evaluated.NullStandin.NOT_NULL || message.hasField(fieldDescriptor))) {
            Object value = message.getField(fieldDescriptor);
            if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                    TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                value = TupleFieldsHelper.fromProto((Message)value, fieldDescriptor.getMessageType());
            }
            // ignore FanType
            return Collections.singletonList(Key.Evaluated.scalar(value));
        } else {
            return getNullResult();
        }
    }

    /**
     * Evaluates the case where no value can be extracted for this field. This method is called from
     * {@link #evaluateMessage} in three cases:
     * <ul>
     * <li>The input {@code message} is {@code null}.
     * <li>The {@code fieldDescriptor} is {@code null} (meaning the field does not exist, i.e., incorrect metadata).
     * <li>For a singular (non-{@code repeated}), {@code optional} field when that field is absent on the message, and
     *     the {@link #nullStandin} is not {@code NOT_NULL} (i.e., the type-default substitution does not apply).
     * </ul>
     * See {@link FieldKeyExpression} for a detailed explanation of the semantics.
     */
    private List<Key.Evaluated> getNullResult() {
        switch (fanType) {
            case FanOut:
                return Collections.emptyList();
            case Concatenate:
                Key.Evaluated result = (nullStandin == Key.Evaluated.NullStandin.NOT_NULL)
                                       ? Key.Evaluated.scalar(Collections.emptyList())
                                       : Key.Evaluated.scalar(nullStandin);
                return Collections.singletonList(result);
            case None:
                return Collections.singletonList(Key.Evaluated.scalar(nullStandin));
            default:
                throw new RecordCoreException("unknown fan type").addLogInfo(LogMessageKeys.VALUE, fanType);
        }
    }

    @Override
    public boolean createsDuplicates() {
        return fanType == FanType.FanOut;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.Field toProto() throws SerializationException {
        return RecordKeyExpressionProto.Field.newBuilder()
                .setFieldName(fieldName)
                .setFanType(fanType.toProto())
                .setNullInterpretation(nullStandin.toProto())
                .build();
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.KeyExpression toKeyExpression() {
        return RecordKeyExpressionProto.KeyExpression.newBuilder().setField(toProto()).build();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Nonnull
    public Quantifier.ForEach explodeField(@Nonnull Quantifier.ForEach baseQuantifier, @Nonnull List<String> fieldNamePrefix) {
        final List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(ProtoUtils.toUserIdentifier(fieldName))
                .build();
        switch (fanType) {
            case FanOut:
                return Quantifier.forEach(Reference.initialOf(
                        ExplodeExpression.explodeField(baseQuantifier, fieldNames)));
            default:
                throw new RecordCoreException("unrecognized fan type");
        }
    }

    @Nonnull
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Nest a single scalar field inside of this one.
     * Shorthand to {@code nest(field(fieldName))}
     * @param fieldName the name of the nested field. This is contextual, there is no need to use the full path.
     * @return a new expression that will get the value from the value of the given field
     * for each value of this field.
     */
    @Nonnull
    public NestingKeyExpression nest(@Nonnull String fieldName) {
        return nest(fieldName, FanType.None);
    }

    /**
     * Nest a single field inside of this one, optionally setting the handling for a repeated field.
     * Shorthand to {@code nest(field(fieldName, fanType))}
     * @param fieldName the name of the nested field
     * @param fanType how to handle the nested field repeated state
     * @return a new key that will get all of the values from the value of the given field within this field
     */
    @Nonnull
    public NestingKeyExpression nest(@Nonnull String fieldName, @Nonnull FanType fanType) {
        return nest(Key.Expressions.field(fieldName, fanType));
    }

    /**
     * Shorthand for <code>nest(concat(first, second, rest))</code>.
     * @param first the first child expression to use
     * @param second the second child expression to use
     * @param rest this supports any number children (at least 2), this is the rest of them
     * @return a new key that will get all of the values from the value of the given field within this field
     */
    @Nonnull
    public NestingKeyExpression nest(@Nonnull KeyExpression first, @Nonnull KeyExpression second,
                                     @Nonnull KeyExpression... rest) {
        return nest(Key.Expressions.concat(first, second, rest));
    }

    /**
     * Nest another expression inside this one. That one will be evaluated for each value of this field in the
     * context of this field. This field must be pointed to a Message. The child also has no access to the parts of
     * the record outside of this field value.
     * If this field is repeated with FanOut, there will be one index entry for each value of this field.
     * @param child a child expression to be evaluated on the message that this field has
     * @return a new expression for evaluating this complicated nesting
     * @throws InvalidExpressionException if this field is of concatenate type
     * At least for now.
     */
    @Nonnull
    public NestingKeyExpression nest(@Nonnull KeyExpression child) {
        // TODO make this work. This is sensible, i.e. if you have:
        // Record { repeated person { firstname, lastname } }
        // You would end up with someting like [ Bob Smith Alice Jackson ]
        if (fanType == FanType.Concatenate) {
            throw new InvalidExpressionException("Concatenated fields cannot nest");
        }
        return new NestingKeyExpression(this, child);
    }

    /**
     * Get this field as a group without any grouping keys.
     * @return this field without any grouping keys
     */
    @Nonnull
    public GroupingKeyExpression ungrouped() {
        return new GroupingKeyExpression(this, 1);
    }

    @Nonnull
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Nonnull
    public SplitKeyExpression split(int splitSize) {
        return new SplitKeyExpression(this, splitSize);
    }

    @Nonnull
    public Descriptors.Descriptor getDescriptor(@Nonnull Descriptors.Descriptor parentDescriptor) {
        final Descriptors.FieldDescriptor field = parentDescriptor.findFieldByName(fieldName);
        return field.getMessageType();
    }

    @Nonnull
    public FanType getFanType() {
        return fanType;
    }

    @Nonnull
    public Key.Evaluated.NullStandin getNullStandin() {
        return nullStandin;
    }

    @Override
    public String toString() {
        return "Field { '" + fieldName + "' " + fanType + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        // TODO: this shouldn't be necessary, figure out how to match the passed root expression without modifying.
        if (o == null || !(o instanceof FieldKeyExpression)) {
            return false;
        }

        // Note that the NullStandIn value is NOT checked here. It will be replaced with
        // https://github.com/FoundationDB/fdb-record-layer/issues/677
        FieldKeyExpression that = (FieldKeyExpression)o;
        return this.fieldName.equals(that.fieldName) &&
               this.fanType == that.fanType;
    }

    @Override
    public int hashCode() {
        // Note that the NullStandIn is NOT included in the hash code. It will be replaced with
        // https://github.com/FoundationDB/fdb-record-layer/issues/677
        return fieldName.hashCode() + fanType.name().hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                // Note that the NullStandIn is NOT included in the hash code. It will be replaced with
                // https://github.com/FoundationDB/fdb-record-layer/issues/677
                return fieldName.hashCode() + fanType.name().hashCode();
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, fieldName, fanType);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }
}

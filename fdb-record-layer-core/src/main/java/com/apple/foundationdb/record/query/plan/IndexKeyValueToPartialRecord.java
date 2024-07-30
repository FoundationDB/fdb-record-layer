/*
 * IndexKeyValueToPartialRecord.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.InvertibleFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopier;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PFieldCopier;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PMessageCopier;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PTupleSource;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.tuple.Tuple;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Construct a record from a covering index.
 */
@API(API.Status.INTERNAL)
public class IndexKeyValueToPartialRecord implements PlanHashable, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Key-Value-To-Partial-Record");
    @Nonnull
    private final List<Copier> copiers;
    private final boolean isRequired;

    private IndexKeyValueToPartialRecord(@Nonnull List<Copier> copiers, boolean isRequired) {
        this.copiers = copiers;
        this.isRequired = isRequired;
    }

    @Nonnull
    public Message toRecord(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull IndexEntry kv) {
        return Verify.verifyNotNull(toRecordInternal(recordDescriptor, kv));
    }

    @Nullable
    public Message toRecordInternal(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull IndexEntry kv) {
        Message.Builder recordBuilder = DynamicMessage.newBuilder(recordDescriptor);
        boolean allCopiersRefused = true;
        for (Copier copier : copiers) {
            if (copier.copy(recordDescriptor, recordBuilder, kv)) {
                allCopiersRefused = false;
            }
        }
        if (isRequired) {
            // If any of the copiers refused since they were asked to copy a null into a required field, this following
            // build call will fail with an exception.
            return recordBuilder.build();
        }
        return allCopiersRefused ? null : recordBuilder.build();
    }

    @Override
    public String toString() {
        return copiers.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexKeyValueToPartialRecord that = (IndexKeyValueToPartialRecord) o;
        return Objects.equals(copiers, that.copiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(BASE_HASH, copiers, isRequired);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, copiers, isRequired);
    }

    @Nonnull
    @Override
    public PIndexKeyValueToPartialRecord toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PIndexKeyValueToPartialRecord.newBuilder();
        for (final Copier copier : copiers) {
            builder.addCopiers(copier.toCopierProto(serializationContext));
        }
        builder.setIsRequired(isRequired);
        return builder.build();
    }

    @Nonnull
    public static IndexKeyValueToPartialRecord fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PIndexKeyValueToPartialRecord indexKeyValueToPartialRecordProto) {
        Verify.verify(indexKeyValueToPartialRecordProto.hasIsRequired());
        final ImmutableList.Builder<Copier> copiersBuilder = ImmutableList.builder();
        for (int i = 0; i < indexKeyValueToPartialRecordProto.getCopiersCount(); i ++) {
            copiersBuilder.add(Copier.fromCopierProto(serializationContext, indexKeyValueToPartialRecordProto.getCopiers(i)));
        }
        return new IndexKeyValueToPartialRecord(copiersBuilder.build(), indexKeyValueToPartialRecordProto.getIsRequired());
    }

    @Nullable
    public static Object getForOrdinalPath(@Nonnull Tuple tuple, @Nonnull final ImmutableIntArray ordinalPath) {
        Object value = tuple;
        for (int i = 0; i < ordinalPath.length(); i ++) {
            if (value instanceof Tuple) {
                value = ((Tuple)value).get(ordinalPath.get(i));
            } else {
                value = ((List<?>)value).get(ordinalPath.get(i));
            }
            if (value == null) {
                return null;
            }
        }
        return value;
    }

    public static boolean existsSubTupleForOrdinalPath(@Nonnull Tuple tuple, @Nonnull final ImmutableIntArray ordinalPath) {
        Object value = tuple;
        for (int i = 0; i < ordinalPath.length(); i ++) {
            if (value instanceof Tuple) {
                value = ((Tuple)value).get(ordinalPath.get(i));
            } else {
                value = ((List<?>)value).get(ordinalPath.get(i));
            }
            if (value == null) {
                break;
            }
        }
        return (value instanceof Tuple) || (value instanceof List<?>);
    }

    /**
     * Which side of the {@link IndexEntry} to take a field from.
     */
    public enum TupleSource {
        KEY, VALUE, OTHER;

        @Nonnull
        @SuppressWarnings("unused")
        PTupleSource toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case KEY:
                    return PTupleSource.KEY;
                case VALUE:
                    return PTupleSource.VALUE;
                case OTHER:
                    return PTupleSource.OTHER;
                default:
                    throw new RecordCoreException("unknown tuple source mapping. did you forget to add it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        static TupleSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PTupleSource tupleSourceProto) {
            switch (tupleSourceProto) {
                case KEY:
                    return KEY;
                case VALUE:
                    return VALUE;
                case OTHER:
                    return OTHER;
                default:
                    throw new RecordCoreException("unknown tuple source mapping. did you forget to add it?");
            }
        }
    }

    /**
     * Copy from an index entry into part of a record.
     */
    public interface Copier extends PlanHashable, PlanSerializable {
        boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                     @Nonnull IndexEntry kv);

        @Nonnull
        PCopier toCopierProto(@Nonnull PlanSerializationContext serializationContext);

        @Nonnull
        static Copier fromCopierProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PCopier copierProto) {
            return (Copier)PlanSerialization.dispatchFromProtoContainer(serializationContext, copierProto);
        }
    }

    /**
     * Copier for basic fields.
     */
    public static class FieldCopier implements Copier {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Copier");

        @Nonnull
        private final String field;
        @Nonnull
        private final TupleSource source;
        @Nonnull
        private final AvailableFields.CopyIfPredicate copyIfPredicate;
        @Nonnull
        private final ImmutableIntArray ordinalPath;
        @Nullable
        private final String invertibleFunctionName;
        @Nullable
        private Function<Object, Object> invertibleFunction;

        private FieldCopier(@Nonnull final String field,
                            @Nonnull final TupleSource source,
                            @Nonnull final AvailableFields.CopyIfPredicate copyIfPredicate,
                            @Nonnull final ImmutableIntArray ordinalPath,
                            @Nullable final String invertibleFunctionName) {
            this.field = field;
            this.source = source;
            this.copyIfPredicate = copyIfPredicate;
            this.ordinalPath = ordinalPath;
            this.invertibleFunctionName = invertibleFunctionName;
        }

        @Override
        public boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                            @Nonnull IndexEntry kv) {
            final Tuple tuple = (source == TupleSource.KEY ? kv.getKey() : kv.getValue());
            if (!copyIfPredicate.test(tuple)) {
                return false;
            }

            Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            Object value = getForOrdinalPath(tuple, ordinalPath);
            if (invertibleFunctionName != null) {
                value = getInvertibleFunction().apply(value);
            }
            if (value == null) {
                //
                // The logic here goes like this: If the field is null, but it is required we cannot
                // copy the value as it is required to be non-null. If it is null, but it is optional or repeated,
                // null is assumed to be the default and the field remains unset.
                //
                return !fieldDescriptor.isRequired();
            }
            switch (fieldDescriptor.getType()) {
                case INT32:
                    value = ((Long)value).intValue();
                    break;
                case BYTES:
                    value = ZeroCopyByteString.wrap((byte[])value);
                    break;
                case MESSAGE:
                    value = TupleFieldsHelper.toProto(value, fieldDescriptor.getMessageType());
                    break;
                case ENUM:
                    value = fieldDescriptor.getEnumType().findValueByNumber(((Long)value).intValue());
                    break;
                default:
                    break;
            }
            recordBuilder.setField(fieldDescriptor, value);
            return true;
        }

        @Nonnull
        public Function<Object, Object> getInvertibleFunction() {
            if (invertibleFunction == null) {
                final InvertibleFunctionKeyExpression keyExpression = (InvertibleFunctionKeyExpression)
                        Key.Expressions.function(Objects.requireNonNull(invertibleFunctionName), Key.Expressions.field(field));
                invertibleFunction = obj -> Iterables.getOnlyElement(keyExpression.evaluateInverse(Key.Evaluated.scalar(obj))).getObject(0);
            }
            return invertibleFunction;
        }

        @Override
        public String toString() {
            return field + ": " + source + ordinalPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldCopier that = (FieldCopier) o;
            return ordinalPath.equals(that.ordinalPath) &&
                    Objects.equals(field, that.field) &&
                    source == that.source &&
                    Objects.equals(invertibleFunctionName, that.invertibleFunctionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, source.name(), ordinalPath, invertibleFunctionName);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, field, source, copyIfPredicate, ordinalPath) +
                    // Keep compatible if absent.
                    PlanHashable.objectPlanHash(hashMode, invertibleFunctionName);
        }

        @Nonnull
        @Override
        public PFieldCopier toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PFieldCopier.Builder builder = PFieldCopier.newBuilder()
                    .setField(field)
                    .setSource(source.toProto(serializationContext))
                    .setCopyIfPredicate(copyIfPredicate.toCopyIfPredicateProto(serializationContext));
            ordinalPath.forEach(builder::addOrdinalPath);
            if (invertibleFunctionName != null) {
                builder.setInvertibleFunction(invertibleFunctionName);
            }
            return builder.build();
        }

        @Nonnull
        @Override
        public PCopier toCopierProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCopier.newBuilder().setFieldCopier(toProto(serializationContext)).build();
        }

        @Nonnull
        public static FieldCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PFieldCopier fieldCopierProto) {
            final ImmutableIntArray.Builder ordinalPathBuilder = ImmutableIntArray.builder();
            for (int i = 0; i < fieldCopierProto.getOrdinalPathCount(); i ++) {
                ordinalPathBuilder.add(fieldCopierProto.getOrdinalPath(i));
            }

            return new FieldCopier(Objects.requireNonNull(fieldCopierProto.getField()),
                    TupleSource.fromProto(serializationContext, Objects.requireNonNull(fieldCopierProto.getSource())),
                    AvailableFields.CopyIfPredicate.fromCopyIfPredicateProto(serializationContext, Objects.requireNonNull(fieldCopierProto.getCopyIfPredicate())),
                    ordinalPathBuilder.build(),
                    fieldCopierProto.hasInvertibleFunction() ? fieldCopierProto.getInvertibleFunction() : null);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PFieldCopier, FieldCopier> {
            @Nonnull
            @Override
            public Class<PFieldCopier> getProtoMessageClass() {
                return PFieldCopier.class;
            }

            @Nonnull
            @Override
            public FieldCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PFieldCopier fieldCopierProto) {
                return FieldCopier.fromProto(serializationContext, fieldCopierProto);
            }
        }
    }

    /**
     * Copier for nested messages.
     */
    public static class MessageCopier implements Copier {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Message-Copier");

        // This might be simpler with Message.Builder.getFieldBuilder(), where the message Builder
        // itself keeps track of partially built submessages, rather than this class having to. But
        // DynamicMessage does not support that.
        @Nonnull
        private final String field;
        @Nonnull
        private final IndexKeyValueToPartialRecord nested;

        private MessageCopier(@Nonnull String field, @Nonnull IndexKeyValueToPartialRecord nested) {
            this.field = field;
            this.nested = nested;
        }

        @Override
        public boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                            @Nonnull IndexEntry kv) {
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            switch (fieldDescriptor.getType()) {
                case MESSAGE:
                    break;
                default:
                    throw new RecordCoreException("only nested message should be handled by MessageCopier");
            }
            final Message message = nested.toRecordInternal(fieldDescriptor.getMessageType(), kv);
            if (message == null) {
                return false;
            }
            if (fieldDescriptor.isRepeated()) {
                recordBuilder.addRepeatedField(fieldDescriptor, message);
            } else {
                recordBuilder.setField(fieldDescriptor, message);
            }
            return true;
        }

        @Override
        public String toString() {
            return field + ": " + nested;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MessageCopier that = (MessageCopier) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(nested, that.nested);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, nested);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, field, nested);
        }

        @Nonnull
        @Override
        public PMessageCopier toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMessageCopier.newBuilder()
                    .setField(field)
                    .setNested(nested.toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PCopier toCopierProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCopier.newBuilder().setMessageCopier(toProto(serializationContext)).build();
        }

        @Nonnull
        public static MessageCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PMessageCopier messageCopierProto) {
            return new MessageCopier(Objects.requireNonNull(messageCopierProto.getField()),
                    IndexKeyValueToPartialRecord.fromProto(serializationContext, Objects.requireNonNull(messageCopierProto.getNested())));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMessageCopier, MessageCopier> {
            @Nonnull
            @Override
            public Class<PMessageCopier> getProtoMessageClass() {
                return PMessageCopier.class;
            }

            @Nonnull
            @Override
            public MessageCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PMessageCopier messageCopierProto) {
                return MessageCopier.fromProto(serializationContext, messageCopierProto);
            }
        }
    }

    public static Builder newBuilder(@Nonnull RecordType recordType) {
        return new Builder(recordType, true);
    }

    public static Builder newBuilder(@Nonnull Descriptors.Descriptor recordDescriptor) {
        return new Builder(recordDescriptor, true);
    }

    /**
     * A builder for {@link IndexKeyValueToPartialRecord}.
     */
    public static class Builder {
        @Nonnull
        private final Descriptors.Descriptor recordDescriptor;
        @Nonnull
        private final Map<String, FieldCopier> fields;
        @Nonnull
        private final Map<String, Builder> nestedBuilders;
        private final List<Copier> regularCopiers = new ArrayList<>();

        private final boolean isRequired;

        private Builder(@Nonnull RecordType recordType, boolean isRequired) {
            this(recordType.getDescriptor(), isRequired);
        }

        private Builder(@Nonnull Descriptors.Descriptor recordDescriptor, boolean isRequired) {
            this.recordDescriptor = recordDescriptor;
            this.fields = new TreeMap<>();
            this.nestedBuilders = new TreeMap<>();
            this.isRequired = isRequired;
        }
        
        public boolean hasField(@Nonnull String field) {
            return fields.containsKey(field) || nestedBuilders.containsKey(field);
        }

        public Builder addField(@Nonnull final String field, @Nonnull final TupleSource source,
                                @Nonnull final AvailableFields.CopyIfPredicate copyIfPredicate,
                                @Nonnull final ImmutableIntArray ordinalPath,
                                @Nullable final String invertibleFunction) {
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            if (fieldDescriptor == null) {
                throw new MetaDataException("field not found: " + field);
            }
            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE &&
                    !TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                throw new RecordCoreException("must set nested message field-by-field: " + field);
            }
            FieldCopier copier = new FieldCopier(field, source, copyIfPredicate, ordinalPath, invertibleFunction);
            FieldCopier prev = fields.put(field, copier);
            if (prev != null) {
                throw new RecordCoreException("setting field more than once: " + field);
            }
            return this;
        }

        public Builder getFieldBuilder(@Nonnull String field) {
            Builder builder = nestedBuilders.get(field);
            if (builder == null) {
                final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
                if (fieldDescriptor == null) {
                    throw new MetaDataException("field not found: " + field);
                }
                if (fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
                    throw new RecordCoreException("not a nested message: " + field);
                }
                builder = new Builder(fieldDescriptor.getMessageType(), fieldDescriptor.isRequired());
                nestedBuilders.put(field, builder);
            }
            return builder;
        }

        public void addRequiredMessageFields() {
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    nestedBuilders.putIfAbsent(fieldDescriptor.getName(), new Builder(fieldDescriptor.getMessageType(), true));
                }
            }
            nestedBuilders.values().forEach(Builder::addRequiredMessageFields);
        }

        /**
         * To be valid for covering index use, must set all required fields and not attempt to set repeated fields,
         * for which the index only has a partial view.
         * @return whether this is a valid use
         */
        public boolean isValid() {
            return isValid(false);
        }

        public boolean isValid(boolean allowRepeated) {
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && !hasField(fieldDescriptor.getName())) {
                    return false;
                }
                if (!allowRepeated && fieldDescriptor.isRepeated() && hasField(fieldDescriptor.getName())) {
                    return false;
                }
                if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    Builder builder = nestedBuilders.get(fieldDescriptor.getName());
                    if (builder != null && !builder.isValid(allowRepeated)) {
                        return false;
                    }
                }
            }
            return true;
        }

        public void addRegularCopier(@Nonnull Copier copier) {
            this.regularCopiers.add(copier);
        }
        
        public IndexKeyValueToPartialRecord build() {
            List<Copier> copiers = new ArrayList<>(fields.values());
            for (Map.Entry<String, Builder> entry : nestedBuilders.entrySet()) {
                copiers.add(new MessageCopier(entry.getKey(), entry.getValue().build()));
            }
            copiers.addAll(regularCopiers);
            return new IndexKeyValueToPartialRecord(copiers, isRequired);
        }
    }
}

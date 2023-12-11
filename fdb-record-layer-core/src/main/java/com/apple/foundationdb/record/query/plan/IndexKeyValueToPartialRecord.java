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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
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
import java.util.function.Predicate;

/**
 * Construct a record from a covering index.
 */
@API(API.Status.INTERNAL)
public class IndexKeyValueToPartialRecord {
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
        return Objects.hash(copiers);
    }

    @SuppressWarnings("UnstableApiUsage")
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

    @SuppressWarnings("UnstableApiUsage")
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
        KEY, VALUE, OTHER
    }

    /**
     * Copy from an index entry into part of a record.
     */
    public interface Copier {
        boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                     @Nonnull IndexEntry kv);
    }

    @SuppressWarnings("UnstableApiUsage")
    static class FieldCopier implements Copier {
        @Nonnull
        private final String field;
        @Nonnull
        private final TupleSource source;
        @Nonnull
        private final Predicate<Tuple> copyIfPredicate;
        @Nonnull
        private final ImmutableIntArray ordinalPath;
        private final Descriptors.FieldDescriptor fieldDescriptor;

        private FieldCopier(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor,
                            @Nonnull final String field,
                            @Nonnull final TupleSource source,
                            @Nonnull final Predicate<Tuple> copyIfPredicate,
                            @Nonnull final ImmutableIntArray ordinalPath) {
            this.field = field;
            this.source = source;
            this.copyIfPredicate = copyIfPredicate;
            this.ordinalPath = ordinalPath;
            this.fieldDescriptor = fieldDescriptor;
        }

        @Override
        public boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                            @Nonnull IndexEntry kv) {
            final Tuple tuple = (source == TupleSource.KEY ? kv.getKey() : kv.getValue());
            if (!copyIfPredicate.test(tuple)) {
                return false;
            }

            Object value = getForOrdinalPath(tuple, ordinalPath);
            if (value == null) {
                return true;
            }
            Descriptors.FieldDescriptor mutableFieldDescriptor = this.fieldDescriptor;
            if (!fieldDescriptor.getContainingType().equals(recordDescriptor)) {
                mutableFieldDescriptor = recordDescriptor.findFieldByName(field);
            }
            switch (mutableFieldDescriptor.getType()) {
                case INT32:
                    value = ((Long)value).intValue();
                    break;
                case BYTES:
                    value = ZeroCopyByteString.wrap((byte[])value);
                    break;
                case MESSAGE:
                    value = TupleFieldsHelper.toProto(value, mutableFieldDescriptor.getMessageType());
                    break;
                case ENUM:
                    value = mutableFieldDescriptor.getEnumType().findValueByNumber(((Long)value).intValue());
                    break;
                default:
                    break;
            }
            recordBuilder.setField(mutableFieldDescriptor, value);
            return true;
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
                    source == that.source;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, source, ordinalPath);
        }
    }

    // This might be simpler with Message.Builder.getFieldBuilder(), where the message Builder
    // itself keeps track of partially built submessages, rather than this class having to. But
    // DynamicMessage does not support that.
    static class MessageCopier implements Copier {
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

        @SuppressWarnings("UnstableApiUsage")
        public Builder addField(@Nonnull String field, @Nonnull TupleSource source, @Nonnull final Predicate<Tuple> copyIfPredicate, @Nonnull ImmutableIntArray ordinalPath) {
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            if (fieldDescriptor == null) {
                throw new MetaDataException("field not found: " + field);
            }
            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE &&
                    !TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                throw new RecordCoreException("must set nested message field-by-field: " + field);
            }
            FieldCopier copier = new FieldCopier(fieldDescriptor, field, source, copyIfPredicate, ordinalPath);
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
                    System.out.println("required field is not present:" + fieldDescriptor.getName());
                    System.out.println("nested descriptor:" + recordDescriptor.toProto());
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

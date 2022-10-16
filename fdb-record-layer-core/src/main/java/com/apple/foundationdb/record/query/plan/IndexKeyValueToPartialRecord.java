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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Construct a record from a covering index.
 */
@API(API.Status.INTERNAL)
public class IndexKeyValueToPartialRecord {
    @Nonnull
    private final List<Copier> copiers;

    private IndexKeyValueToPartialRecord(@Nonnull final List<Copier> copiers) {
        this.copiers = copiers;
    }

    public Message toRecord(@Nonnull final Descriptors.Descriptor recordDescriptor, @Nonnull final IndexEntry kv) {
        Message.Builder recordBuilder = DynamicMessage.newBuilder(recordDescriptor);
        for (final Copier copier : copiers) {
            copier.copy(recordDescriptor, recordBuilder, kv);
        }
        return recordBuilder.build();
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
        void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                  @Nonnull IndexEntry kv);
    }

    static class FieldCopier implements Copier {
        @Nonnull
        private final FieldAccessor.Bound field;
        @Nonnull
        private final TupleSource source;
        private final int index;
        private Descriptors.FieldDescriptor fieldDescriptor;
        private Descriptors.Descriptor containingType;

        private FieldCopier(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor, @Nonnull FieldAccessor.Bound field, @Nonnull TupleSource source, int index) {
            this.field = field;
            this.source = source;
            this.index = index;
            this.fieldDescriptor = fieldDescriptor;
            this.containingType = fieldDescriptor.getContainingType();
        }

        @Override
        public void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                         @Nonnull IndexEntry kv) {
            final Tuple tuple = (source == TupleSource.KEY ? kv.getKey() : kv.getValue());
            Object value = tuple.get(index);
            if (value == null) {
                return;
            }
            if (!containingType.equals(recordDescriptor)) {
                containingType = recordDescriptor;
                fieldDescriptor = field.getField(recordDescriptor);
            }
            switch (fieldDescriptor.getType()) {
                case INT32:
                    value = ((Long)value).intValue();
                    break;
                case BYTES:
                    value = ByteString.copyFrom((byte[])value);
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
        }

        @Override
        public String toString() {
            return field + ": " + source + "[" + index + "]";
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
            return index == that.index &&
                    Objects.equals(field, that.field) &&
                    source == that.source;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, source, index);
        }
    }

    // This might be simpler with Message.Builder.getFieldBuilder(), where the message Builder
    // itself keeps track of partially built submessages, rather than this class having to. But
    // DynamicMessage does not support that.
    static class MessageCopier implements Copier {
        @Nonnull
        private final FieldAccessor.Bound field;
        @Nonnull
        private final IndexKeyValueToPartialRecord nested;

        private MessageCopier(@Nonnull FieldAccessor.Bound field, @Nonnull IndexKeyValueToPartialRecord nested) {
            this.field = field;
            this.nested = nested;
        }

        @Override
        public void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                         @Nonnull IndexEntry kv) {
            final Descriptors.FieldDescriptor fieldDescriptor = field.getField(recordDescriptor);
            switch (fieldDescriptor.getType()) {
                case MESSAGE:
                    break;
                default:
                    throw new RecordCoreException("only nested message should be handled by MessageCopier");
            }
            final Message message = nested.toRecord(fieldDescriptor.getMessageType(), kv);
            if (fieldDescriptor.isRepeated()) {
                recordBuilder.addRepeatedField(fieldDescriptor, message);
            } else {
                recordBuilder.setField(fieldDescriptor, message);
            }
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

    /**
     * Creates a new {@link Builder}.
     * @param recordType The record type.
     * @return a new {@link Builder}.
     * @deprecated this method is deprecated, for new usages use {@link Builder#newBuilderUsingFieldName(RecordType)}.
     */
    @Deprecated
    public static Builder<String> newBuilder(@Nonnull RecordType recordType) {
        return new Builder<>(recordType, FieldAccessor.byName());
    }

    public static Builder<String> newBuilderUsingFieldName(@Nonnull RecordType recordType) {
        return new Builder<>(recordType, FieldAccessor.byName());
    }

    public static Builder<Integer> newBuilderUsingFieldIndex(@Nonnull RecordType recordType) {
        return new Builder<>(recordType, FieldAccessor.byIndex());
    }

    /**
     * A builder for {@link IndexKeyValueToPartialRecord}.
     */
    public static class Builder<T extends Comparable<T>> {
        @Nonnull
        private final Descriptors.Descriptor recordDescriptor;
        @Nonnull
        private final Map<T, FieldCopier> fields;
        @Nonnull
        private final Map<T, Builder<T>> nestedBuilders;
        @Nonnull
        private final List<Copier> regularCopiers = new ArrayList<>();
        @Nonnull
        private final FieldAccessor<T> fieldAccessor;

        private Builder(@Nonnull RecordType recordType,
                        @Nonnull final FieldAccessor<T> fieldAccessor) {
            this(recordType.getDescriptor(), fieldAccessor);
        }

        private Builder(@Nonnull Descriptors.Descriptor recordDescriptor,
                        @Nonnull final FieldAccessor<T> fieldAccessor) {
            this.recordDescriptor = recordDescriptor;
            this.fieldAccessor = fieldAccessor;
            this.fields = new TreeMap<>();
            this.nestedBuilders = new TreeMap<>();
        }
        
        public boolean hasField(@Nonnull T field) {
            return fields.containsKey(field) || nestedBuilders.containsKey(field);
        }

        public Builder addField(@Nonnull T field, @Nonnull TupleSource source, int index) {
            final Descriptors.FieldDescriptor fieldDescriptor = fieldAccessor.getField(field, recordDescriptor);
            if (fieldDescriptor == null) {
                throw new MetaDataException("field not found: " + field);
            }
            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE &&
                    !TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                throw new RecordCoreException("must set nested message field-by-field: " + field);
            }
            FieldCopier copier = new FieldCopier(fieldDescriptor, fieldAccessor.bind(field), source, index);
            FieldCopier prev = fields.put(field, copier);
            if (prev != null) {
                throw new RecordCoreException("setting field more than once: " + field);
            }
            return this;
        }

        public Builder<T> getFieldBuilder(@Nonnull T field) {
            Builder<T> builder = nestedBuilders.get(field);
            if (builder == null) {
                final Descriptors.FieldDescriptor fieldDescriptor = fieldAccessor.getField(field, recordDescriptor);
                if (fieldDescriptor == null) {
                    throw new MetaDataException("field not found: " + field);
                }
                if (fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
                    throw new RecordCoreException("not a nested message: " + field);
                }
                builder = new Builder<>(fieldDescriptor.getMessageType(), fieldAccessor);
                nestedBuilders.put(field, builder);
            }
            return builder;
        }

        public void addRequiredMessageFields() {
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    nestedBuilders.putIfAbsent(fieldAccessor.getFieldIdentifier(fieldDescriptor),
                            new Builder<T>(fieldDescriptor.getMessageType(), fieldAccessor));
                }
            }
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
            for (final Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && !hasField(fieldAccessor.getFieldIdentifier(fieldDescriptor))) {
                    return false;
                }
                if (!allowRepeated && fieldDescriptor.isRepeated() && hasField(fieldAccessor.getFieldIdentifier(fieldDescriptor))) {
                    return false;
                }
                if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    final Builder<T> builder = nestedBuilders.get(fieldAccessor.getFieldIdentifier(fieldDescriptor));
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
            final List<Copier> copiers = new ArrayList<>(fields.values());
            for (final Map.Entry<T, Builder<T>> entry : nestedBuilders.entrySet()) {
                copiers.add(new MessageCopier(fieldAccessor.bind(entry.getKey()), entry.getValue().build()));
            }
            copiers.addAll(regularCopiers);
            return new IndexKeyValueToPartialRecord(copiers);
        }
    }

    interface FieldAccessor<T extends Comparable<T>> {

        interface Bound {

            Descriptors.FieldDescriptor getField(@Nonnull Descriptors.Descriptor record);

            abstract class BoundFieldAccessorImpl<T extends Comparable<T>> implements Bound {
                final T modifier;

                private BoundFieldAccessorImpl(final T modifier) {
                    this.modifier = modifier;
                }

                @Override
                public boolean equals(final Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    final var that = (BoundFieldAccessorImpl<?>)o;
                    return modifier == that.modifier;
                }

                @Override
                public int hashCode() {
                    return Objects.hash(modifier);
                }

            }

            static BoundFieldAccessorImpl<Integer> get(int modifier) {
                return new BoundFieldAccessorImpl<>(modifier) {
                    @Override
                    public Descriptors.FieldDescriptor getField(@Nonnull final Descriptors.Descriptor record) {
                        return record.findFieldByNumber(modifier);
                    }
                };
            }

            static BoundFieldAccessorImpl<String> get(@Nonnull final String modifier) {
                return new BoundFieldAccessorImpl<>(modifier) {
                    @Override
                    public Descriptors.FieldDescriptor getField(@Nonnull final Descriptors.Descriptor record) {
                        return record.findFieldByName(modifier);
                    }
                };
            }

        }
        Descriptors.FieldDescriptor getField(@Nonnull final T modifier, @Nonnull Descriptors.Descriptor record);

        T getFieldIdentifier(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor);

        Bound bind(@Nonnull final T modifier);

        class ByIndex implements FieldAccessor<Integer> {
            @Override
            public Descriptors.FieldDescriptor getField(@Nonnull final Integer modifier, @Nonnull final Descriptors.Descriptor record) {
                return record.findFieldByNumber(modifier);
            }

            @Override
            public Integer getFieldIdentifier(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor) {
                return fieldDescriptor.getNumber();
            }

            @Override
            public Bound bind(@Nonnull final Integer modifier) {
                return Bound.get(modifier);
            }
        }

        class ByName implements FieldAccessor<String> {
            @Override
            public Descriptors.FieldDescriptor getField(@Nonnull final String modifier, @Nonnull final Descriptors.Descriptor record) {
                return record.findFieldByName(modifier);
            }

            @Override
            public String getFieldIdentifier(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor) {
                return fieldDescriptor.getName();
            }

            @Override
            public Bound bind(@Nonnull final String modifier) {
                return Bound.get(modifier);
            }
        }

        @Nonnull
        static ByIndex byIndex() {
            return new ByIndex();
        }

        @Nonnull
        static ByName byName() {
            return new ByName();
        }
    }
}

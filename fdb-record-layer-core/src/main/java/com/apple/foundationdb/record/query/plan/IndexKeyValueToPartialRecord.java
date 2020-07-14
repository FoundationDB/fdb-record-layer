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

    private IndexKeyValueToPartialRecord(@Nonnull List<Copier> copiers) {
        this.copiers = copiers;
    }

    public Message toRecord(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull IndexEntry kv) {
        Message.Builder recordBuilder = DynamicMessage.newBuilder(recordDescriptor);
        for (Copier copier : copiers) {
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
        KEY, VALUE
    }

    interface Copier {
        void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                  @Nonnull IndexEntry kv);
    }

    static class FieldCopier implements Copier {
        @Nonnull
        private final String field;
        @Nonnull
        private final TupleSource source;
        private final int index;

        private FieldCopier(@Nonnull String field, @Nonnull TupleSource source, int index) {
            this.field = field;
            this.source = source;
            this.index = index;
        }

        @Override
        public void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                         @Nonnull IndexEntry kv) {
            final Tuple tuple = (source == TupleSource.KEY ? kv.getKey() : kv.getValue());
            Object value = tuple.get(index);
            if (value == null) {
                return;
            }
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
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
        private final String field;
        @Nonnull
        private final IndexKeyValueToPartialRecord nested;

        private MessageCopier(@Nonnull String field, @Nonnull IndexKeyValueToPartialRecord nested) {
            this.field = field;
            this.nested = nested;
        }

        @Override
        public void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                         @Nonnull IndexEntry kv) {
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            switch (fieldDescriptor.getType()) {
                case MESSAGE:
                    break;
                default:
                    throw new RecordCoreException("only nested message should be handled by MessageCopier");
            }
            recordBuilder.setField(fieldDescriptor, nested.toRecord(fieldDescriptor.getMessageType(), kv));
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

    public static Builder newBuilder(@Nonnull Descriptors.Descriptor recordDescriptor) {
        return new Builder(recordDescriptor);
    }

    static class Builder {
        @Nonnull
        private final Descriptors.Descriptor recordDescriptor;
        @Nonnull
        private final Map<String, FieldCopier> fields;
        @Nonnull
        private final Map<String, Builder> nestedBuilders;

        private Builder(@Nonnull Descriptors.Descriptor recordDescriptor) {
            this.recordDescriptor = recordDescriptor;
            this.fields = new TreeMap<>();
            this.nestedBuilders = new TreeMap<>();
        }
        
        public boolean hasField(@Nonnull String field) {
            return fields.containsKey(field) || nestedBuilders.containsKey(field);
        }

        public Builder addField(@Nonnull String field, @Nonnull TupleSource source, int index) {
            final Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(field);
            if (fieldDescriptor == null) {
                throw new MetaDataException("field not found: " + field);
            }
            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE &&
                    !TupleFieldsHelper.isTupleField(fieldDescriptor.getMessageType())) {
                throw new RecordCoreException("must set nested message field-by-field: " + field);
            }
            FieldCopier copier = new FieldCopier(field, source, index);
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
                builder = new Builder(fieldDescriptor.getMessageType());
                nestedBuilders.put(field, builder);
            }
            return builder;
        }

        public void addRequiredMessageFields() {
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    nestedBuilders.putIfAbsent(fieldDescriptor.getName(), new Builder(fieldDescriptor.getMessageType()));
                }
            }
        }

        /**
         * To be valid for covering index use, must set all required fields and not attempt to set repeated fields,
         * for which the index only has a partial view.
         */
        public boolean isValid() {
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() && !hasField(fieldDescriptor.getName())) {
                    return false;
                }
                if (fieldDescriptor.isRepeated() && hasField(fieldDescriptor.getName())) {
                    return false;
                }
                if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    Builder builder = nestedBuilders.get(fieldDescriptor.getName());
                    if (builder != null && !builder.isValid()) {
                        return false;
                    }
                }
            }
            return true;
        }
        
        public IndexKeyValueToPartialRecord build() {
            List<Copier> copiers = new ArrayList<>(fields.values());
            for (Map.Entry<String, Builder> entry : nestedBuilders.entrySet()) {
                copiers.add(new MessageCopier(entry.getKey(), entry.getValue().build()));
            }
            return new IndexKeyValueToPartialRecord(copiers);
        }
    }
}

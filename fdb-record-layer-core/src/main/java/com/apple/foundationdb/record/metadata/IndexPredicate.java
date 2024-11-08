/*
 * IndexPredicate.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This is a Plain old Java object (POJO) hierarchy representing SerDe operations on a predicate on an {@link Index}.
 * It resembles very closely a subset of {@link QueryPredicate} type hierarchy, and offers a way of conversion to
 * {@link IndexPredicate} (see {@link IndexPredicate#toPredicate(Value)}).
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexPredicate {

    private final Map<String, QueryPredicate> queryPredicateMap = new ConcurrentHashMap<>();

    /**
     * Check if a given record should be indexed.
     * @param <M> the subtype of {@link Message}
     * @param store record store
     * @param savedRecord the updated record
     * @return true if this index should generate index entries for this record
     *
     * Note that for now, IndexPredicate does not support filtering of certain index entries.
     */
    public <M extends Message> boolean shouldIndexThisRecord(@Nonnull FDBRecordStore store, @Nonnull final FDBIndexableRecord<M> savedRecord) {
        CorrelationIdentifier objectQuantifier = Quantifier.current();
        QueryPredicate queryPredicate = getQueryPredicate(store.getRecordMetaData(), savedRecord.getRecordType(), objectQuantifier);

        String bindingName = Bindings.Internal.CORRELATION.bindingName(objectQuantifier.getId());
        Bindings bindings = Bindings.newBuilder().set(bindingName, QueryResult.ofComputed(savedRecord.getRecord())).build();

        return Boolean.TRUE.equals(queryPredicate.eval(store, EvaluationContext.forBindings(bindings)));
    }

    private QueryPredicate getQueryPredicate(RecordMetaData metaData, RecordType type, CorrelationIdentifier objectQuantifier) {
        final String typeName = type.getName();
        final String keyName = typeName + "#" + objectQuantifier.getId();
        return queryPredicateMap.computeIfAbsent(keyName, ignored -> {
            final RecordType recordType = metaData.getRecordType(typeName);
            Type.Record typeRecord = Type.Record.fromDescriptor(recordType.getDescriptor());
            Value recordValue = QuantifiedObjectValue.of(objectQuantifier, typeRecord);
            return toPredicate(recordValue);
        });
    }

    /**
     * Parses a proto message into a corresponding predicate.
     * @param proto The serialized protobuf representation of the {@link IndexPredicate}.
     * @return a deserialized {@link IndexPredicate}.
     * @throws RecordCoreException if the provided message is not supported.
     */
    @Nonnull
    public static IndexPredicate fromProto(@Nonnull final RecordMetaDataProto.Predicate proto) {
        if (proto.hasAndPredicate()) {
            return new AndPredicate(proto.getAndPredicate());
        } else if (proto.hasOrPredicate()) {
            return new OrPredicate(proto.getOrPredicate());
        } else if (proto.hasConstantPredicate()) {
            return new ConstantPredicate(proto.getConstantPredicate());
        } else if (proto.hasNotPredicate()) {
            return new NotPredicate(proto.getNotPredicate());
        } else if (proto.hasValuePredicate()) {
            return new ValuePredicate(proto.getValuePredicate());
        } else {
            throw new RecordCoreException("attempt to deserialize unsupported predicate").addLogInfo(LogMessageKeys.VALUE, proto);
        }
    }

    /**
     * Parses a {@link QueryPredicate} into an equivalent {@link IndexPredicate}.
     * @param queryPredicate The query predicate to convert.
     * @return an equivalent {@link IndexPredicate}.
     * @throws RecordCoreException if the provided query predicate is not supported.
     */
    @VisibleForTesting
    @Nonnull
    public static IndexPredicate fromQueryPredicate(@Nonnull final QueryPredicate queryPredicate) {
        if (queryPredicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate) {
            return new ConstantPredicate((com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate)queryPredicate);
        } else if (queryPredicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate) {
            return new NotPredicate((com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate)queryPredicate);
        } else if (queryPredicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate) {
            return new AndPredicate((com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate)queryPredicate);
        } else if (queryPredicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate) {
            return new OrPredicate((com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate)queryPredicate);
        } else if (queryPredicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate) {
            return new ValuePredicate((com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate)queryPredicate);
        } else {
            throw new RecordCoreException("attempt to construct index predicate PoJo from unsupported query predicate").addLogInfo(LogMessageKeys.VALUE, queryPredicate);
        }
    }

    /**
     * Checks whether a {@link QueryPredicate} is supported for SerDe operations using this POJO hierarchy.
     *
     * @param predicate The predicate to check.
     * @return {@code true} if the predicate is supported, otherwise {@code false}.
     */
    public static boolean isSupported(@Nonnull final QueryPredicate predicate) {
        if (predicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate) {
            return true;
        } else if (predicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate) {
            return isSupported(((com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate)predicate).child);
        } else if (predicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate) {
            return StreamSupport.stream(predicate.getChildren().spliterator(), false).allMatch(IndexPredicate::isSupported);
        } else if (predicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate) {
            return StreamSupport.stream(predicate.getChildren().spliterator(), false).allMatch(IndexPredicate::isSupported);
        } else if (predicate instanceof com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate) {
            final var valuePredicate = (com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate)predicate;
            return IndexComparison.isSupported(valuePredicate.getComparison()) &&
                   valuePredicate.getValue() instanceof FieldValue &&
                   ((FieldValue)valuePredicate.getValue()).getFieldPathNamesMaybe().stream().allMatch(Optional::isPresent);
        } else {
            return false;
        }
    }

    /**
     * Serializes a POJO into the corresponding protobuf message.
     *
     * @return an equivalent protobuf message.
     */
    @Nonnull
    public abstract RecordMetaDataProto.Predicate toProto();

    /**
     * Converts a POJO into an equivalent {@link QueryPredicate}.
     *
     * @param value The base value needed for parsing a {@link ValuePredicate} into a {@link FieldValue}.
     * @return an equivalent {@link QueryPredicate}.
     */
    @Nonnull
    public abstract QueryPredicate toPredicate(@Nonnull Value value);

    /**
     * A POJO equivalent for {@link com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate}.
     */
    public static class AndPredicate extends IndexPredicate {
        @Nonnull
        private final List<IndexPredicate> children;

        public AndPredicate(@Nonnull final Collection<IndexPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public AndPredicate(@Nonnull final RecordMetaDataProto.AndPredicate proto) {
            this.children = proto.getChildrenList().stream().map(IndexPredicate::fromProto).collect(Collectors.toList());
        }

        @VisibleForTesting
        public AndPredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate predicate) {
            this.children = predicate.getChildren().stream().map(IndexPredicate::fromQueryPredicate).collect(Collectors.toList());
        }

        @Nonnull
        public List<IndexPredicate> getChildren() {
            return children;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            // TODO memoize
            final var andPredicateProto = RecordMetaDataProto.AndPredicate.newBuilder();
            children.forEach(child -> andPredicateProto.addChildren(child.toProto()));
            return RecordMetaDataProto.Predicate.newBuilder().setAndPredicate(andPredicateProto.build()).build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate(@Nonnull final Value value) {
            return com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate.and(children.stream().map(c -> c.toPredicate(value)).collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return '(' + children.stream().map(IndexPredicate::toString).collect(Collectors.joining(" AND ")) + ") ";
        }
    }

    /**
     * A POJO equivalent for {@link com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate}.
     */
    public static class OrPredicate extends IndexPredicate {
        @Nonnull
        private final List<IndexPredicate> children;

        public OrPredicate(@Nonnull final Collection<IndexPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public OrPredicate(@Nonnull final RecordMetaDataProto.OrPredicate proto) {
            this.children = proto.getChildrenList().stream().map(IndexPredicate::fromProto).collect(Collectors.toList());
        }

        @VisibleForTesting
        public OrPredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate predicate) {
            this.children = predicate.getChildren().stream().map(IndexPredicate::fromQueryPredicate).collect(Collectors.toList());
        }

        @Nonnull
        public List<IndexPredicate> getChildren() {
            return children;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            final var orPredicateProto = RecordMetaDataProto.OrPredicate.newBuilder();
            children.forEach(child -> orPredicateProto.addChildren(child.toProto()));
            return RecordMetaDataProto.Predicate.newBuilder().setOrPredicate(orPredicateProto.build()).build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate(@Nonnull final Value value) {
            return com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate.or(children.stream().map(c -> c.toPredicate(value)).collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return '(' + children.stream().map(IndexPredicate::toString).collect(Collectors.joining(" OR ")) + ") ";
        }
    }

    /**
     * a POJO equivalent for {@link com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate}.
     */
    public static class ConstantPredicate extends IndexPredicate {
        /**
         * The constant to use.
         */
        public enum ConstantValue {
            TRUE,
            FALSE,
            NULL
        }

        @Nonnull
        private final ConstantValue value;

        public ConstantPredicate(@Nonnull final ConstantValue value) {
            this.value = value;
        }

        @VisibleForTesting
        @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
        public ConstantPredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate predicate) {
            if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.TRUE) {
                this.value = ConstantValue.TRUE;
            } else if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.FALSE) {
                this.value = ConstantValue.FALSE;
            } else if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.NULL) {
                this.value = ConstantValue.NULL;
            }
            throw new RecordCoreException("could not create a PoJo constant index predicate").addLogInfo(LogMessageKeys.VALUE, predicate);
        }

        public ConstantPredicate(@Nonnull final RecordMetaDataProto.ConstantPredicate proto) {
            switch (proto.getValue()) {
                case TRUE:
                    this.value = ConstantValue.TRUE;
                    break;
                case FALSE:
                    this.value = ConstantValue.FALSE;
                    break;
                case NULL:
                    this.value = ConstantValue.NULL;
                    break;
                default:
                    throw new RecordCoreException("attempt to deserialize unknown constant predicate value").addLogInfo(LogMessageKeys.VALUE, proto.getValue());
            }
        }

        @Nonnull
        public ConstantValue getValue() {
            return value;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            RecordMetaDataProto.ConstantPredicate.ConstantValue protoValue;
            switch (value) {
                case TRUE:
                    protoValue = RecordMetaDataProto.ConstantPredicate.ConstantValue.TRUE;
                    break;
                case FALSE:
                    protoValue = RecordMetaDataProto.ConstantPredicate.ConstantValue.FALSE;
                    break;
                case NULL:
                    protoValue = RecordMetaDataProto.ConstantPredicate.ConstantValue.NULL;
                    break;
                default:
                    throw new RecordCoreException("attempt to serialize unsupported value").addLogInfo(LogMessageKeys.VALUE, value);
            }
            return RecordMetaDataProto.Predicate.newBuilder()
                    .setConstantPredicate(RecordMetaDataProto.ConstantPredicate.newBuilder()
                            .setValue(protoValue)
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate(@Nonnull final Value value) {
            switch (this.value) {
                case TRUE:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.TRUE;
                case FALSE:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.FALSE;
                case NULL:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.NULL;
                default:
                    throw new RecordCoreException("attempt to serialize unsupported value").addLogInfo(LogMessageKeys.VALUE, this.value);
            }
        }

        @Override
        public String toString() {
            return value.name() + ' ';
        }
    }

    /**
     * A POJO equivalent of {@link com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate}.
     */
    public static class NotPredicate extends IndexPredicate {
        @Nonnull
        private final IndexPredicate value;

        public NotPredicate(@Nonnull final IndexPredicate value) {
            this.value = value;
        }

        public NotPredicate(@Nonnull final RecordMetaDataProto.NotPredicate notPredicate) {
            this.value = IndexPredicate.fromProto(notPredicate.getChild());
        }

        @VisibleForTesting
        NotPredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate predicate) {
            value = IndexPredicate.fromQueryPredicate(predicate.child);
        }

        @Nonnull
        public IndexPredicate getValue() {
            return value;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            return RecordMetaDataProto.Predicate.newBuilder()
                    .setNotPredicate(RecordMetaDataProto.NotPredicate.newBuilder()
                            .setChild(value.toProto())
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate(@Nonnull final Value value) {
            return com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate.not(this.value.toPredicate(value));
        }

        @Override
        public String toString() {
            return "(NOT " + value + ") ";
        }
    }

    /**
     * A POJO equivalent of {@link com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate}.
     */
    public static class ValuePredicate extends IndexPredicate {
        @Nonnull
        private final List<String> fieldPath;

        @Nonnull
        private final IndexComparison comparison;

        public ValuePredicate(@Nonnull final List<String> fieldPath, @Nonnull final IndexComparison comparison) {
            this.fieldPath = ImmutableList.copyOf(fieldPath);
            this.comparison = comparison;
        }

        @VisibleForTesting
        @SuppressWarnings("java:S5803")
        ValuePredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate predicate) {
            Verify.verify(predicate.getValue() instanceof FieldValue);
            this.fieldPath = ImmutableList.copyOf(((FieldValue)predicate.getValue()).getFieldPathNames());
            this.comparison = IndexComparison.fromComparison(predicate.getComparison());
        }

        public ValuePredicate(@Nonnull final RecordMetaDataProto.ValuePredicate proto) {
            Verify.verify(proto.getValueCount() > 0, "attempt to deserialize %s without value", ValuePredicate.class.getSimpleName());
            Verify.verify(proto.hasComparison(), "attempt to deserialize %s without comparison", ValuePredicate.class.getSimpleName());
            this.fieldPath = ImmutableList.copyOf(proto.getValueList());
            this.comparison = IndexComparison.fromProto(proto.getComparison());
        }

        @Nonnull
        public List<String> getFieldPath() {
            return fieldPath;
        }

        @Nonnull
        public IndexComparison getComparison() {
            return comparison;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Predicate toProto() {
            return RecordMetaDataProto.Predicate.newBuilder()
                    .setValuePredicate(RecordMetaDataProto.ValuePredicate.newBuilder()
                            .addAllValue(fieldPath)
                            .setComparison(comparison.toProto())
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate(@Nonnull final Value value) {
            return new com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate(FieldValue.ofFieldNames(value, fieldPath), comparison.toComparison());
        }

        @Override
        public String toString() {
            return '(' + fieldPath.stream().collect(Collectors.joining("/")) + ' ' + comparison + ") ";
        }
    }
}

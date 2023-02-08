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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.KeyExpressionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This is a simple PoJo hierarchy representing SerDe operations on a predicate on an {@link Index}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexPredicate {

    @Nonnull
    public static IndexPredicate fromProto(@Nonnull final RecordMetaDataProto.Predicate proto, @Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
        if (proto.hasAndPredicate()) {
            return new AndPredicate(proto.getAndPredicate(), alias, inputType);
        } else if (proto.hasOrPredicate()) {
            return new OrPredicate(proto.getOrPredicate(), alias, inputType);
        } else if (proto.hasConstantPredicate()) {
            return new ConstantPredicate(proto.getConstantPredicate());
        } else if (proto.hasNotPredicate()) {
            return new NotPredicate(proto.getNotPredicate(), alias, inputType);
        } else if (proto.hasValuePredicate()) {
            return new ValuePredicate(proto.getValuePredicate(), alias, inputType);
        } else {
            throw new RecordCoreException(String.format("attempt to deserialize not supported predicate '%s'", proto));
        }
    }

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
            throw new RecordCoreException(String.format("attempt to construct index predicate PoJo fro unsupported query predicate '%s'", queryPredicate));
        }
    }

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
            return IndexComparison.isSupported(((com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate)predicate).getComparison());
        } else {
            return false;
        }
    }

    @Nonnull
    public abstract RecordMetaDataProto.Predicate toProto();

    @Nonnull
    public abstract QueryPredicate toPredicate();

    static class AndPredicate extends IndexPredicate {
        @Nonnull
        private final List<IndexPredicate> children;

        public AndPredicate(@Nonnull final Collection<IndexPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public AndPredicate(@Nonnull final RecordMetaDataProto.AndPredicate proto, @Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
            this.children = proto.getChildrenList().stream().map(c -> IndexPredicate.fromProto(c, alias, inputType)).collect(Collectors.toList());
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
            // TODO (yhatem) memoize
            final var andPredicateProto = RecordMetaDataProto.AndPredicate.newBuilder();
            children.forEach(child -> andPredicateProto.addChildren(child.toProto()));
            return RecordMetaDataProto.Predicate.newBuilder().setAndPredicate(andPredicateProto.build()).build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate() {
            return new com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate(children.stream().map(c -> c.toPredicate()).collect(Collectors.toList()));
        }
    }

    static class OrPredicate extends IndexPredicate {
        @Nonnull
        private final List<IndexPredicate> children;

        public OrPredicate(@Nonnull final Collection<IndexPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public OrPredicate(@Nonnull final RecordMetaDataProto.OrPredicate proto, final CorrelationIdentifier alias, final Type inputType) {
            this.children = proto.getChildrenList().stream().map(c -> IndexPredicate.fromProto(c, alias, inputType)).collect(Collectors.toList());
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
            // TODO (yhatem) memoize
            final var orPredicateProto = RecordMetaDataProto.OrPredicate.newBuilder();
            children.forEach(child -> orPredicateProto.addChildren(child.toProto()));
            return RecordMetaDataProto.Predicate.newBuilder().setOrPredicate(orPredicateProto.build()).build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate() {
            return new com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate(children.stream().map(c -> c.toPredicate()).collect(Collectors.toList()));
        }
    }

    static class ConstantPredicate extends IndexPredicate {
        enum ConstantValue {
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
        public ConstantPredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate predicate) {
            if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.TRUE) {
                this.value = ConstantValue.TRUE;
            } else if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.FALSE) {
                this.value = ConstantValue.FALSE;
            } else if (predicate == com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.NULL) {
                this.value = ConstantValue.NULL;
            }
            throw new RecordCoreException(String.format("could not create a PoJo constant index predicate from '%s'", predicate));
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
                    throw new RecordCoreException(String.format("attempt to deserialize unknown constant predicate value '%s'", proto.getValue()));
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
                    throw new RecordCoreException(String.format("attempt to serialize unsupported value '%s'", value));
            }
            return RecordMetaDataProto.Predicate.newBuilder()
                    .setConstantPredicate(RecordMetaDataProto.ConstantPredicate.newBuilder()
                            .setValue(protoValue)
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate() {
            switch (value) {
                case TRUE:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.TRUE;
                case FALSE:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.FALSE;
                case NULL:
                    return com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate.NULL;
                default:
                    throw new RecordCoreException(String.format("attempt to serialize unsupported value '%s'", value));
            }
        }
    }

    static class NotPredicate extends IndexPredicate {
        @Nonnull
        private final IndexPredicate value;

        NotPredicate(@Nonnull final IndexPredicate value) {
            this.value = value;
        }

        NotPredicate(@Nonnull final RecordMetaDataProto.NotPredicate notPredicate, final CorrelationIdentifier alias, final Type inputType) {
            this.value = IndexPredicate.fromProto(notPredicate.getChild(), alias, inputType);
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
        public QueryPredicate toPredicate() {
            return new com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate(value.toPredicate());
        }
    }

    static class ValuePredicate extends IndexPredicate {
        @Nonnull
        private final Value value;

        @Nonnull
        private final IndexComparison comparison;

        ValuePredicate(@Nonnull final Value value, @Nonnull final IndexComparison comparison) {
            this.value = value;
            this.comparison = comparison;
        }

        @VisibleForTesting
        ValuePredicate(@Nonnull final com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate predicate) {
            this.value = predicate.getValue();
            this.comparison = IndexComparison.fromComparison(predicate.getComparison());
        }

        ValuePredicate(@Nonnull final RecordMetaDataProto.ValuePredicate proto, @Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
            Verify.verify(proto.hasValue(), String.format("attempt to deserialize %s without value", com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate.class));
            Verify.verify(proto.hasComparison(), String.format("attempt to deserialize %s without comparison", com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate.class));
            final var keyExpression = KeyExpression.fromProto(proto.getValue());
            this.value = new ScalarTranslationVisitor(keyExpression).toResultValue(alias, inputType);
            this.comparison = IndexComparison.fromProto(proto.getComparison());
        }

        @Nonnull
        public Value getValue() {
            return value;
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
                            .setValue(KeyExpressionUtils.toKeyExpression(value).toKeyExpression())
                            .setComparison(comparison.toProto())
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public QueryPredicate toPredicate() {
            return new com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate(value, comparison.toComparison());
        }
    }

    /**
     * Provides a (cached) instance of {@link IndexPredicate}.
     */
    public static class IndexPredicateProvider {

        @Nonnull
        private final AtomicReference<RecordMetaDataProto.Predicate> proto;

        @Nonnull
        private final AtomicReference<IndexPredicate> indexPredicateReference;

        private IndexPredicateProvider(@Nonnull final IndexPredicate predicate) {
            this.proto = new AtomicReference<>();
            this.indexPredicateReference = new AtomicReference<>(predicate);
        }

        private IndexPredicateProvider(@Nonnull final RecordMetaDataProto.Predicate proto) {
            this.proto = new AtomicReference<>(proto);
            this.indexPredicateReference = new AtomicReference<>(null);
        }

        @Nonnull
        IndexPredicate getIndexPredicate(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
            if (indexPredicateReference.get() != null) {
                return indexPredicateReference.get();
            }
            indexPredicateReference.compareAndSet(null, IndexPredicate.fromProto(Objects.requireNonNull(proto.get()), alias, inputType));
            return indexPredicateReference.get();
        }

        @Nonnull
        RecordMetaDataProto.Predicate toProto() {
            if (proto.get() != null) {
                return proto.get();
            }
            proto.compareAndSet(null, indexPredicateReference.get().toProto());
            return proto.get();
        }

        @Nonnull
        public static IndexPredicateProvider getInstance(@Nonnull final RecordMetaDataProto.Predicate proto) {
            return new IndexPredicateProvider(proto);
        }

        @Nonnull
        public static IndexPredicateProvider getInstance(@Nonnull final IndexPredicate predicate) {
            return new IndexPredicateProvider(predicate);
        }

    }
}

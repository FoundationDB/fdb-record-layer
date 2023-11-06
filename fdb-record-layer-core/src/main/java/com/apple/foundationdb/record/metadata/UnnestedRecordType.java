/*
 * UnnestedRecordType.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@linkplain SyntheticRecordType synthetic record type} representing an unnesting of some kind of
 * repeated, nested structure. Each record of this type should be associated with one stored record
 * type. The other constituents represent nested messages that can be extracted from the main parent
 * record. For example, suppose one had a record type with the following protobuf definition:
 *
 * <pre>{@code
 * message OuterType {
 *     message NestedInnerMessage {
 *         optional int64 a = 1;
 *         optional string b = 2;
 *     }
 *     repeated NestedInnerMessage nested_repeated = 1;
 *     optional int64 other_field = 2;
 * }
 * }</pre>
 *
 * <p>
 * Then an {@code UnnestedRecordType} may be defined that is constructed with two constituents,
 * one of which refers to the {@code OuterType} and the other contains one {@code NestedInnerMessage}. This
 * can then be used to define indexes that would not normally be possible using other key expressions. For
 * example, consider the expression:
 * </p>
 *
 * <pre>
 *
 * concat(field("nested_repeated", FanOut).nest("a"), field("other_field"), field("nested_repeated", FanOut).nest("b"))
 * </pre>
 *
 * <p>
 * In this expression, the first field ({@code nested_repeated.a}) and the third field ({@code nested_repeated.b}) are
 * not associated with the same {@code NestedInnerMessage}. So, if there are <em>n</em> items in that repeated list,
 * there will end up being <em>n</em><sup>2</sup> different values returned from evaluating this expression. By
 * contrast, this expression:
 * </p>
 *
 * <pre>
 *
 * concat(field("other_field"), field("nested_repeated", FanOut).nest("a", "b"))
 * </pre>
 *
 * <p>
 * Evaluates to only <em>n</em>, as in every evaluated entry, the {@code a} and {@code b} fields come from the same
 * {@code NestedInnerMessage}. However, to acheive this, we have had to swap the order of {@code other_field} and
 * {@code nested_repeated.a}.
 * </p>
 *
 * <p>
 * If we want to retain the original order but we only want one entry per {@code NestedInnerMessage}, we can
 * do this with an unnested type. We associate two constituents with the type: one of them, {@code "parent"},
 * is used to refer to the {@code OuterType}. The other, {@code "child"}, can be used to refer to the results of the
 * {@code nested_repeated} field:
 * </p>
 *
 * <pre> {@code
 * var typeBuilder = metaDataBuilder.addUnnestedRecordType(typeName);
 * typeBuilder.addParentConstituent("parent", metaDataBuilder.getRecordType("OuterType"));
 * typeBuilder.addNestedConstituent("child", NestedInnerMessage.getDescriptor(), "parent", field("nested_repeated", FanType.FanOut));
 * }</pre>
 *
 * <p>
 * Then one can define an index on the expression:
 * </p>
 *
 * <pre>
 *
 * concat(field("child").nest("a"), field("parent").nest("other_field"), field("child").nest("b"))
 * </pre>
 *
 * <p>
 * Will contain precisely one entry for every {@code NestedInnerMessage} (like the second expression) and the fields
 * will be in the same order as the original expression.
 * </p>
 *
 * <h2>Primary Keys</h2>
 *
 * <p>
 * The primary key structure for this record type is similar to other {@link SyntheticRecordType}s, but unlike s
 * {@link JoinedRecordType}, where every constituent is a member of a proper {@link RecordType} which has a well-defined
 * primary key, the non-parent constituents of an {@code UnnestedRecordType} do not have their own primary key
 * expression to evaluate. For this reason, each synthetic record's primary key consists of:
 * </p>
 *
 * <ol>
 *     <li>The record type key</li>
 *     <li>Then the parent constituent record's primary key as a nested {@link Tuple}</li>
 *     <li>For each nested record constituent, a single nested {@link Tuple} containing the index of
 *          the constituent within the list of values returned by evaluating the nesting expression against the
 *          constituent's parent</li>
 * </ol>
 *
 * <p>
 * For example, using the {@code OuterType} as above, if the {@code OuterType}'s primary key is on
 * {@code field("other_field")}, and its primary key might conceptually look something like:
 * </p>
 *
 * <pre>
 * Tuple.from(
 *     type.getRecordTypeKey(),
 *     Tuple.from(outerMessage.getOtherField()),
 *     Tuple.from(outerMessage.getNestedRepeatedList().indexOf(nestedInnerMessage))
 * )
 * </pre>
 *
 * <p>
 * Note that if the same value appears multiple times for the same nested constituent, each occurrence is given
 * its own index and so it will generate a different {@link FDBSyntheticRecord}. Note that this also means that
 * if the order of the elements changes, then the synthetic records will similarly be changed.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class UnnestedRecordType extends SyntheticRecordType<UnnestedRecordType.NestedConstituent> {
    /**
     * Special field in the synthetic record that contains the positions of all of the nested constituents. It will
     * contain a nested message field that has one field for all of the constituents (except for the parent constituent)
     * containing the index of that constituent within the nesting from its parent.
     */
    @Nonnull
    public static final String POSITIONS_FIELD = "__positions";

    @Nonnull
    private final NestedConstituent parentConstituent;

    /**
     * A {@link SyntheticRecordType.Constituent} representing an element in an unnested record. This encodes
     * both the name of the nested messages making up the synthetic type and the relationship between them.
     * There should always be one {@linkplain #isParent() parent} constituent, and all other cnstituents
     * are linked to a different constituent via a {@linkplain #getNestingExpression() nesting expression}.
     */
    public static class NestedConstituent extends SyntheticRecordType.Constituent {
        @Nullable
        private final NestedConstituent parent;
        @Nonnull
        private final KeyExpression nestingExpression;

        NestedConstituent(@Nonnull final String name, @Nonnull final RecordType recordType,
                                    @Nullable final NestedConstituent parent,
                                    @Nonnull KeyExpression nestingExpression) {
            super(name, recordType);
            this.parent = parent;
            this.nestingExpression = nestingExpression;
        }

        /**
         * The immediate parent of this constituent. If this is the record's
         * {@linkplain #isParent() parent constituent}, this will return {@code null}.
         *
         * @return this constituent's parent
         */
        @Nullable
        public NestedConstituent getParent() {
            return parent;
        }

        /**
         * Get the name of this constituent's {@linkplain #getParent() parent}. This will
         * return {@code null} if the parent is {@code null}.
         *
         * @return the name of this constituent's parent of {@code null}
         */
        @Nullable
        public String getParentName() {
            return parent == null ? null : parent.getName();
        }

        /**
         * Expression that can generate instances of this constituent when evaluated on an instance
         * of its parent. For the type's {@linkplain #isParent() parent constituent}, this is unused.
         *
         * @return an expression linking the parent constituent to this constituent
         */
        @Nonnull
        public KeyExpression getNestingExpression() {
            return nestingExpression;
        }

        /**
         * Whether this is the parent of the record type. This should be {@code true} for exactly
         * one constituent on the type. This parent should be some {@link FDBStoredRecord} of a type
         * defined in the meta-data, and all other constituents should be derived by evaluating the
         * {@linkplain #getNestingExpression() nesting expression} on the parent constituent.
         *
         * @return whether this is the single parent constituent
         */
        public boolean isParent() {
            return parent == null;
        }
    }

    @SuppressWarnings("squid:S107") // allow more constructor parameters as builder for type exists
    protected UnnestedRecordType(@Nonnull final RecordMetaData metaData,
                                 @Nonnull final Descriptors.Descriptor descriptor,
                                 @Nonnull final KeyExpression primaryKey,
                                 @Nonnull final Object recordTypeKey,
                                 @Nonnull final List<Index> indexes,
                                 @Nonnull final List<Index> multiTypeIndexes,
                                 @Nonnull final List<NestedConstituent> constituents
                                 /*@Nonnull final List<Nesting> nestings*/) {
        super(metaData, descriptor, primaryKey, recordTypeKey, indexes, multiTypeIndexes, constituents);
        this.parentConstituent = constituents.stream()
                .filter(NestedConstituent::isParent)
                .findFirst()
                .orElseThrow(() -> new MetaDataException("unnested record type missing parent constituent"));
    }

    /**
     * Get the constituent of this type which represents the base {@link FDBStoredRecord}. All other
     * constituents should be nested messages (potentially nested recursively multiple layers) defined
     * on this constituent.
     *
     * @return the parent constituent of this type
     */
    @Nonnull
    public NestedConstituent getParentConstituent() {
        return parentConstituent;
    }

    @Override
    @Nonnull
    @API(API.Status.INTERNAL)
    public CompletableFuture<FDBSyntheticRecord> loadByPrimaryKeyAsync(@Nonnull final FDBRecordStore store, @Nonnull final Tuple primaryKey) {
        Tuple parentPrimaryKey = primaryKey.getNestedTuple(1);
        return store.loadRecordAsync(parentPrimaryKey).thenApply(storedRecord -> {
            Map<String, FDBStoredRecord<?>> constituentValues = new HashMap<>();
            constituentValues.put(getParentConstituent().getName(), storedRecord);

            boolean foundMore = true;
            while (foundMore) {
                foundMore = false;
                for (NestedConstituent constituent : getConstituents()) {
                    if (!constituent.isParent() && constituentValues.containsKey(constituent.getParentName())
                            && !constituentValues.containsKey(constituent.getName())) {
                        foundMore = true;
                        FDBStoredRecord<?> parentRecord = constituentValues.get(constituent.getParentName());
                        List<Key.Evaluated> childElems = constituent.getNestingExpression().evaluate(parentRecord);

                        int childConstituentIndex = getConstituents().indexOf(constituent);
                        int childElemIndex = (int) primaryKey.getNestedTuple(childConstituentIndex + 1).getLong(0);
                        if (childElemIndex >= childElems.size()) {
                            throw new RecordCoreException("child element position is too large")
                                    .addLogInfo(LogMessageKeys.CHILD_COUNT, childElems.size());
                        }
                        Key.Evaluated childElem = childElems.get(childElemIndex);
                        Message childMessage = childElem.getObject(0, Message.class);
                        FDBStoredRecord<?> childRecord = FDBStoredRecord.newBuilder()
                                .setRecordType(constituent.getRecordType())
                                .setRecord(childMessage)
                                .setPrimaryKey(Tuple.from(childElemIndex))
                                .build();
                        constituentValues.put(constituent.getName(), childRecord);
                    }
                }
            }

            return FDBSyntheticRecord.of(this, constituentValues);
        });
    }

    /**
     * Create a protobuf serialization of this record type. This can then be used within a larger meta-data protobuf
     * definition.
     *
     * @return a protobuf serialization of this type
     */
    @Nonnull
    public RecordMetaDataProto.UnnestedRecordType toProto() {
        var builder = RecordMetaDataProto.UnnestedRecordType.newBuilder()
                .setName(getName())
                .setRecordTypeKey(LiteralKeyExpression.toProtoValue(getRecordTypeKey()));

        for (NestedConstituent constituent : getConstituents()) {
            RecordMetaDataProto.UnnestedRecordType.NestedConstituent.Builder constituentBuilder = builder.addNestedConstituentsBuilder()
                    .setName(constituent.getName());

            if (constituent.isParent()) {
                // For the parent constituent, only include the record's type name so that it can be looked up
                // from the meta-data's list of types at deserialization time
                constituentBuilder
                        .setTypeName(constituent.getRecordType().getName());
            } else {
                // For all other constituents, the type name is the full name so that it can be resolved
                // using protobuf resolution rules at deserialization time. Also, include the parent
                // and nesting information
                constituentBuilder
                        .setTypeName(constituent.getRecordType().getDescriptor().getFullName())
                        .setParent(Objects.requireNonNull(constituent.getParentName()))
                        .setNestingExpression(constituent.getNestingExpression().toKeyExpression());
            }
        }
        return builder.build();
    }
}

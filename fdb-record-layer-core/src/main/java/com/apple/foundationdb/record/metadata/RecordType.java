/*
 * RecordType.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Record type meta-data.
 *
 * A record type corresponds to a Protobuf {@link com.google.protobuf.Descriptors.Descriptor} and specifies a primary key expression and any number of secondary {@link Index}es.
 */
@API(API.Status.MAINTAINED)
public class RecordType implements RecordTypeOrBuilder, RecordMetaDataProvider {
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final String name;
    @Nonnull
    private final Descriptors.Descriptor descriptor;
    @Nonnull
    private final KeyExpression primaryKey;
    @Nonnull
    private final List<Index> indexes;
    @Nonnull
    private final List<Index> multiTypeIndexes;
    @Nullable
    private final Integer sinceVersion;
    @Nullable
    private final Object explicitRecordTypeKey;
    @Nullable
    private Object recordTypeKey;
    @Nullable
    private Tuple recordTypeKeyTuple = null;

    public RecordType(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.Descriptor descriptor, @Nonnull KeyExpression primaryKey,
                      @Nonnull List<Index> indexes, @Nonnull List<Index> multiTypeIndexes, @Nullable Integer sinceVersion, @Nullable Object recordTypeKey) {
        this.metaData = metaData;
        this.descriptor = descriptor;
        this.primaryKey = primaryKey;
        this.name = descriptor.getName();
        this.indexes = indexes;
        this.multiTypeIndexes = multiTypeIndexes;
        this.sinceVersion = sinceVersion;
        this.recordTypeKey = this.explicitRecordTypeKey = TupleTypeUtil.toTupleEquivalentValue(recordTypeKey);
    }

    @Override
    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    @Nonnull
    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
    }

    @Override
    @Nonnull
    public List<Index> getIndexes() {
        return indexes;
    }

    /**
     * The Indexes that this record type is on that also contain other record types.
     * This does not include indexes that cover all record types
     * @return a list of all indexes that include this record type along with other types.
     */
    @Override
    @Nonnull
    public List<Index> getMultiTypeIndexes() {
        return multiTypeIndexes;
    }

    /**
     * Gets the list of all indexes that apply for this type.
     * <ul>
     * <li>single type indexes defined on this type</li>
     * <li>multi-type indexes including this type</li>
     * <li>universal indexes</li>
     * </ul>
     * @return the list of indexes for this type
     * @see #getIndexes
     * @see #getMultiTypeIndexes
     * @see RecordMetaData#getUniversalIndexes
     */
    @Nonnull
    public List<Index> getAllIndexes() {
        List<Index> allIndexes = new ArrayList<>();
        allIndexes.addAll(getIndexes());
        allIndexes.addAll(getMultiTypeIndexes());
        allIndexes.addAll(getRecordMetaData().getUniversalIndexes());
        return allIndexes;
    }

    @Override
    @Nonnull
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    @Nullable
    @Override
    public Integer getSinceVersion() {
        return sinceVersion;
    }

    /**
     * Get whether this record type sets an explicit value for {@link #getRecordTypeKey}.
     * If there is no explicit value, then {@code #getRecordTypeKey} will use the union message field number.
     * @return {@code} true if there is an explicit record type key value
     */
    public boolean hasExplicitRecordTypeKey() {
        return explicitRecordTypeKey != null;
    }

    /**
     * Get any explicit record type key value.
     * @return the explicit record type key value or {@code null} if {@link #getRecordTypeKey} would return a union field number
     */
    @Nullable
    public Object getExplicitRecordTypeKey() {
        return explicitRecordTypeKey;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This value is not guaranteed to be {@link Tuple}-encodable. It <em>is</em> guaranteed that the value returned
     * has implemented {@link Object#equals(Object) equals()} and {@link Object#hashCode() hashCode()} so that the
     * value can be used in hash-based data structures like {@link java.util.HashMap HashMap}s or
     * {@link java.util.HashSet HashSet}s. If the value is needed to construct a {@code Tuple} or {@link com.apple.foundationdb.subspace.Subspace Subspace},
     * one should call {@link #getRecordTypeKeyTuple()}.
     * </p>
     *
     * @return stable and unique key for the record type
     */
    @Nonnull
    @Override
    public Object getRecordTypeKey() {
        if (recordTypeKey == null) {
            // Taking the smallest matching field makes this stable if fields are deprecated and not removed.
            recordTypeKey = TupleTypeUtil.toTupleEquivalentValue(
                    metaData.getUnionDescriptor().getFields().stream()
                        .filter(f -> f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && f.getMessageType() == descriptor)
                        .min(Comparator.comparing(Descriptors.FieldDescriptor::getNumber))
                        .orElseThrow(() -> new MetaDataException("no matching fields in union"))
                        .getNumber());
        }
        return recordTypeKey;
    }

    /**
     * Get a {@link Tuple} containing only the {@linkplain #getRecordTypeKey() record type key}. The record type
     * key is not generally required to be {@link Tuple}-encodable, so for some record type keys, the following
     * might throw an error:
     *
     * <pre>{@code
     *    Tuple.from(recordType.getRecordTypeKey()).pack()
     * }</pre>
     *
     * <p>
     * This function will first convert the record type key into a type that the {@code Tuple} layer can encode
     * before returning the {@code Tuple} to the user. This method should therefore be preferred over calling
     * {@link #getRecordTypeKey()} when this key is about to be used to read or write data from the database.
     * </p>
     *
     * @return a {@link Tuple} containing the {@linkplain #getRecordTypeKey() record type key}
     */
    @Nonnull
    public Tuple getRecordTypeKeyTuple() {
        if (recordTypeKeyTuple == null) {
            recordTypeKeyTuple = Tuple.from(TupleTypeUtil.toTupleAppropriateValue(getRecordTypeKey()));
        }
        return recordTypeKeyTuple;
    }

    /**
     * Determine whether this record type has a {@link com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression} prefix in the primary key.
     * @return {@code true} if start of the primary key is the unique record type key
     */
    public boolean primaryKeyHasRecordTypePrefix() {
        return Key.Expressions.hasRecordTypePrefix(primaryKey);
    }

    /**
     * Get the meta-data of which this record type is a part.
     * @return owning meta-data
     */
    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

    /**
     * Get whether this record type represents <i>synthetic</i> records.
     *
     * A synthetic record is one that is not actually stored, although it can may be indexed.
     * @return {@code true} if this record type represents synthetic records, {@code false} if it represents records that are actually stored
     */
    @API(API.Status.EXPERIMENTAL)
    public boolean isSynthetic() {
        return false;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("RecordType {'").append(name).append("'");
        str.append(", ").append(primaryKey);
        str.append("}");
        if (explicitRecordTypeKey != null) {
            str.append("#").append(explicitRecordTypeKey);
        }
        return str.toString();
    }
}

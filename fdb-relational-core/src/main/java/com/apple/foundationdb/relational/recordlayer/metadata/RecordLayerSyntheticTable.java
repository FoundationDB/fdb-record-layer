/*
 * RecordLayerSyntheticTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.SyntheticTable;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for synthetic tables in the relational layer: a virtual table backed by a record-layer synthetic record
 * type ({@code UnnestedRecordType} or {@code JoinedRecordType}) with one or more indexes maintained on it.
 *
 * <p>Two synthetic tables made of the same stored tables, related in the same way, produce the same synthetic records
 * up to a renaming of their constituents, whatever their aliases and the order their constituents were registered in.
 * An index can then be defined on either one, once its columns are renamed accordingly. {@link #getFingerprint()} is
 * equal for such tables, and so is the key to look one up by, see {@link Fingerprint}; the {@code matcher} of each
 * kind decides whether a table of the same fingerprint is such a table and how their constituents correspond.
 *
 * @see RecordLayerUnnestedSyntheticTable
 * @see RecordLayerJoinedSyntheticTable
 */
@API(API.Status.EXPERIMENTAL)
public abstract sealed class RecordLayerSyntheticTable implements SyntheticTable
        permits RecordLayerUnnestedSyntheticTable, RecordLayerJoinedSyntheticTable {

    @Nonnull
    final Type.Record type;

    @Nonnull
    private final Set<RecordLayerIndex> indexes;

    protected RecordLayerSyntheticTable(@Nonnull final Set<RecordLayerIndex> indexes,
                                        @Nonnull final Type.Record type) {
        // Insertion order, unlike Set.copyOf: the record layer numbers the indexes of a type in the order they are
        // visited, so the order has to be the same on every JVM for the metadata to be.
        this.indexes = ImmutableSet.copyOf(indexes);
        this.type = type;
    }

    @Nonnull
    @Override
    public String getName() {
        return Objects.requireNonNull(type.getName());
    }

    @Nonnull
    public Type.Record getType() {
        return type;
    }

    @Nonnull
    @Override
    public Set<RecordLayerIndex> getIndexes() {
        return indexes;
    }

    /**
     * {@return what this table has in common with every synthetic table producing the same synthetic records, up to a
     * renaming of their constituents}
     */
    @Nonnull
    public abstract Fingerprint<?> getFingerprint();

    /**
     * {@return this table, with the given index defined on it as well}
     *
     * @param index the index to add, which has to be named differently from every index already on this table
     */
    @Nonnull
    public abstract RecordLayerSyntheticTable withIndex(@Nonnull RecordLayerIndex index);

    /**
     * {@return the indexes of this table and the given one}
     *
     * @param index the index to add
     */
    @Nonnull
    protected Set<RecordLayerIndex> indexesWith(@Nonnull final RecordLayerIndex index) {
        Assert.thatUnchecked(indexes.stream().noneMatch(existing -> existing.getName().equals(index.getName())),
                ErrorCode.INDEX_ALREADY_EXISTS, "attempt to add duplicate index '%s'", index.getName());
        return ImmutableSet.<RecordLayerIndex>builder().addAll(indexes).add(index).build();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RecordLayerSyntheticTable that = (RecordLayerSyntheticTable) o;
        return Objects.equals(type, that.type) && Objects.equals(indexes, that.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, indexes);
    }

    /**
     * What synthetic tables producing the same synthetic records, up to a renaming of their constituents, have in
     * common, whatever their aliases and the order their constituents were registered in. It is the key synthetic
     * tables are looked up by: a value whose equality is structural, and whose hash code is derived from storage names,
     * counts and key expressions only, so that it is the same on every run and JVM. Each kind seeds its hash code with
     * a {@code BASE_HASH} of its own, so that fingerprints of different kinds hash apart. Tables of equal fingerprints
     * need not produce the same synthetic records; the {@code matcher} of each kind decides that.
     *
     * <p>Only one kind of synthetic table has each kind of fingerprint, so the tables of a fingerprint are all of the
     * kind it names.
     *
     * @param <T> the kind of synthetic table with this kind of fingerprint
     */
    @API(API.Status.EXPERIMENTAL)
    public sealed interface Fingerprint<T extends RecordLayerSyntheticTable>
            permits RecordLayerUnnestedSyntheticTable.UnnestedFingerprint, RecordLayerJoinedSyntheticTable.JoinedFingerprint {
        /**
         * {@return the kind of synthetic table with this kind of fingerprint}
         */
        @Nonnull
        Class<T> getTableClass();
    }

    public interface Builder {
        /**
         * {@return the record type of the synthetic table being built}
         */
        @Nonnull
        Type.Record getType();

        @Nonnull
        Builder addIndex(@Nonnull RecordLayerIndex index);

        @Nonnull
        RecordLayerSyntheticTable build();
    }
}

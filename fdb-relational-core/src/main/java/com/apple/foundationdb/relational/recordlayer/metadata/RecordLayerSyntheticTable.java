/*
 * RecordLayerSyntheticTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.api.metadata.Visitor;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Base class for synthetic record types in the relational layer. A synthetic type is an indexed
 * view — it is defined by a SQL SELECT query (like a {@link View}) and additionally backed by a
 * record-layer synthetic record type ({@code UnnestedRecordType}) with one or more indexes
 * maintained on it.
 *
 * <p>Implementing {@link View} reflects the semantic reality: a synthetic type is a virtual,
 * named, SQL-defined type. It is not temporary, and its description is synthesized from its
 * constituent definitions rather than stored as a raw string.
 *
 * @see RecordLayerUnnestedSyntheticTable
 */
@API(API.Status.EXPERIMENTAL)
public abstract sealed class RecordLayerSyntheticTable implements View
        permits RecordLayerUnnestedSyntheticTable {

    @Nonnull
    private final String name;

    @Nonnull
    private final Set<RecordLayerIndex> indexes;

    protected RecordLayerSyntheticTable(@Nonnull final String name,
                                       @Nonnull final Set<RecordLayerIndex> indexes) {
        this.name = name;
        this.indexes = ImmutableSet.copyOf(indexes);
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public abstract String getDescription();

    /** Synthetic types are always permanent. */
    @Override
    public boolean isTemporary() {
        return false;
    }

    @Nonnull
    public Set<RecordLayerIndex> getIndexes() {
        return indexes;
    }

    /**
     * Dispatches to the typed {@link SkeletonVisitor} method for this concrete synthetic type,
     * then visits each index. Callers that only have a {@link Visitor} reference will reach
     * {@link Visitor#visit(View)} via the {@link View} default.
     */
    @Override
    public void accept(@Nonnull final Visitor visitor) {
        if (visitor instanceof SkeletonVisitor) {
            acceptSkeleton((SkeletonVisitor) visitor);
        } else {
            visitor.visit(this);
        }
    }

    /** Typed dispatch for {@link SkeletonVisitor} — implemented by each concrete subclass. */
    protected void acceptSkeleton(@Nonnull SkeletonVisitor visitor) {
        visitor.visit(this);
        for (final var index : getIndexes()) {
            index.accept(visitor);
        }
    }

    /** Common builder contract for all synthetic type builders. */
    public interface Builder {
        @Nonnull
        Builder addIndex(@Nonnull RecordLayerIndex index);

        @Nonnull
        RecordLayerSyntheticTable build();
    }
}

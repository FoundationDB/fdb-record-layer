/*
 * SyntheticTableGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;

import javax.annotation.Nonnull;

/**
 * Generator for synthetic table an index is defined on - for the index definitions that need one.
 *
 * <p>Each kind decides for itself whether a definition needs it, what it cannot express, and how the index key reads
 * against it. {@link SyntheticTableGeneratorFactory} is the one place that picks between them, and they are mutually
 * exclusive: unnesting within a join does not compose yet.
 */
interface SyntheticTableGenerator {

    /**
     * Rejects what this kind of synthetic table generator cannot work with, separately from the definitions
     * {@link IndexSpec#checkValidity} rejects for any index at all.
     *
     * @param spec what the index is made of
     */
    void checkSupported(@Nonnull IndexSpec spec);

    /**
     * The same index, made of the synthetic table rather than the stored records: the projection and ordering are
     * re-expressed in the synthetic table's coordinates, so that they translate as ordinary field paths.
     *
     * @param spec what the index is made of, resolved against the stored records
     *
     * @return the {@link IndexSpec}, in the synthetic table's coordinates
     */
    @Nonnull
    IndexSpec rewrite(@Nonnull IndexSpec spec);

    @Nonnull
    Type.Record getType();

    @Nonnull
    RecordLayerSyntheticTable.Builder generate();
}

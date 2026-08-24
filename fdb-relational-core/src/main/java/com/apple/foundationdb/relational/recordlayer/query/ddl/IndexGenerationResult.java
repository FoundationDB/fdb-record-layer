/*
 * IndexGenerationResult.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * The outcome of generating an index from a DDL definition.
 *
 * <p>Most indexes are defined on a stored table, in which case only the index definition is produced. An index
 * whose key spans a record composed of several others is instead defined on a synthetic record type, which is
 * returned alongside it: an unnested type for a stored record and the elements of an array it unnests, a joined
 * type for the sides of a join. Callers must register that type, since the index names it, so leaving it out
 * would produce an index on a type that does not exist.
 *
 * @param indexBuilder the index definition
 * @param syntheticType the synthetic type to register, empty for an index on a stored table
 */
@API(API.Status.EXPERIMENTAL)
public record IndexGenerationResult(@Nonnull RecordLayerIndex.Builder indexBuilder,
                                    @Nonnull Optional<RecordLayerSyntheticTable.Builder> syntheticType) {

    /**
     * Registers the generated index on the given schema template, together with the synthetic type it is defined on
     * when there is one. Doing this here rather than at each call site keeps the two from drifting apart: an index
     * whose synthetic type is not registered names a type that does not exist.
     *
     * @param metadataBuilder the schema template being built
     */
    public void registerOn(@Nonnull final RecordLayerSchemaTemplate.Builder metadataBuilder) {
        if (syntheticType.isPresent()) {
            metadataBuilder.addSyntheticTable(syntheticType.get().addIndex(indexBuilder.build()).build());
        } else {
            final RecordLayerIndex index = indexBuilder.build();
            final var table = metadataBuilder.extractTable(index.getTableName());
            metadataBuilder.addTable(RecordLayerTable.Builder.from(table).addIndex(index).build());
        }
    }
}

/*
 * NoTypeKeyDdlFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.CreateSchemaTemplateConstantAction;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

public class NoTypeKeyDdlFactory {
    public static RecordLayerMetadataOperationsFactory.Builder newBuilder() {
        return new RecordLayerMetadataOperationsFactory.Builder() {
            @Override
            public RecordLayerMetadataOperationsFactory build() {
                return new RecordLayerMetadataOperationsFactory(rlConfig, storeCatalog, templateCatalog, baseKeySpace) {
                    @Nonnull
                    @Override
                    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                        Assert.thatUnchecked(template instanceof RecordLayerSchemaTemplate);
                        final var recordLayerSchemaTemplate = (RecordLayerSchemaTemplate) template;
                        final LinkedHashSet<RecordLayerTable> newTables = recordLayerSchemaTemplate.getTables().stream()
                                .map(t -> RecordLayerTable.Builder
                                        .from(t.getType())
                                        .setPrimaryKey(t.getPrimaryKey().getSubKey(1, t.getPrimaryKey().getColumnSize()))
                                        .addIndexes(t.getIndexes())
                                        .addGenerations(t.getGenerations())
                                        .build())
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                        template = RecordLayerSchemaTemplate
                                .newBuilder()
                                .setName(template.getName())
                                .addTables(newTables)
                                .setVersion(template.getVersion())
                                .build();
                        return new CreateSchemaTemplateConstantAction(template, templateCatalog);
                    }
                };
            }
        };
    }
}

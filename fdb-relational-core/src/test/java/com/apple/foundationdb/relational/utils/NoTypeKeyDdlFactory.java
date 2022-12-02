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
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.TableInfo;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerConstantActionFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.SchemaTemplateDescriptor;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

public class NoTypeKeyDdlFactory {
    public static RecordLayerConstantActionFactory.Builder newBuilder() {
        return new RecordLayerConstantActionFactory.Builder() {
            @Override
            public RecordLayerConstantActionFactory build() {
                return new RecordLayerConstantActionFactory(rlConfig, storeCatalog, templateCatalog, baseKeySpace) {
                    @Nonnull
                    @Override
                    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                        LinkedHashSet<TableInfo> newTables = template.getTables().stream()
                                .map(t -> new TableInfo(
                                        t.getTableName(),
                                        t.getPrimaryKey().getSubKey(1, t.getPrimaryKey().getColumnSize()),
                                        t.getIndexes(),
                                        t.toDescriptor()))
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                        template = new SchemaTemplateDescriptor(
                                template.getUniqueId(),
                                newTables,
                                template.getTypes(),
                                Collections.emptySet(),
                                template.getVersion()
                        );
                        return new CreateSchemaTemplateConstantAction(template, templateCatalog);
                    }
                };
            }
        };
    }
}

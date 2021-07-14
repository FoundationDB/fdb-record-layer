/*
 * NoOpConstantActionFactory.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Properties;

public class NoOpConstantActionFactory implements ConstantActionFactory{
    public static final NoOpConstantActionFactory INSTANCE = new NoOpConstantActionFactory();

    private NoOpConstantActionFactory(){}

    @Override
    public @Nonnull ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull String templateName, @Nonnull Properties templateProperties) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getCreateTableConstantAction(@Nonnull String tableName, @Nonnull List<ColumnDescriptor> columns, @Nonnull Properties tableProps) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getCreateValueIndexConstantAction(@Nonnull String indexName, @Nonnull String tableName, @Nonnull List<ValueIndexColumnDescriptor> indexedColumns, @Nonnull Properties tableProps) {
        return NoOpConstantAction.INSTANCE;
    }

    private static class NoOpConstantAction implements ConstantAction {
        private static final NoOpConstantAction INSTANCE = new NoOpConstantAction();
        @Override public void execute() throws RelationalException { }
    }
}

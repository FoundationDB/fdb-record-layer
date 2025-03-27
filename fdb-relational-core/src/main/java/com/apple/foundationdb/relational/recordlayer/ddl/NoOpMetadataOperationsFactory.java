/*
 * NoOpMetadataOperationsFactory.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;

import javax.annotation.Nonnull;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public final class NoOpMetadataOperationsFactory implements MetadataOperationsFactory {
    public static final NoOpMetadataOperationsFactory INSTANCE = new NoOpMetadataOperationsFactory();

    private NoOpMetadataOperationsFactory() {
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate templateName, @Nonnull Options templateProperties) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schemaName, @Nonnull Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    private static class NoOpConstantAction implements ConstantAction {
        private static final NoOpConstantAction INSTANCE = new NoOpConstantAction();

        @Override
        public void execute(Transaction txn) throws RelationalException {
        }
    }
}

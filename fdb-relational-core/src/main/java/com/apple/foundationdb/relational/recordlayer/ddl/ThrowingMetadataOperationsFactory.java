/*
 * ThrowingMetadataOperationsFactory.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * A {@link MetadataOperationsFactory} that rejects every DDL operation by throwing an
 * {@link com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException}
 * at factory-lookup time. Use this in DQL-only contexts (e.g. offline planning) where
 * any DDL invocation should fail fast.
 */
@API(API.Status.EXPERIMENTAL)
public final class ThrowingMetadataOperationsFactory implements MetadataOperationsFactory {
    public static final ThrowingMetadataOperationsFactory INSTANCE = new ThrowingMetadataOperationsFactory();

    private ThrowingMetadataOperationsFactory() {
    }

    private static RuntimeException reject(@Nonnull final String operation) {
        return new RelationalException(
                "DDL operation '" + operation + "' is not allowed in this context",
                ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
    }

    @Nonnull
    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        throw reject("CREATE SCHEMA TEMPLATE");
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
        throw reject("DROP SCHEMA TEMPLATE");
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
        throw reject("CREATE DATABASE");
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
        throw reject("CREATE SCHEMA");
    }

    @Nonnull
    @Override
    public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
        throw reject("DROP DATABASE");
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schemaName, @Nonnull Options options) {
        throw reject("DROP SCHEMA");
    }

    @Nonnull
    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull SchemaTemplate template, boolean throwIfExists, @Nonnull RecordLayerInvokedRoutine invokedRoutine) {
        throw reject("CREATE TEMPORARY FUNCTION");
    }

    @Nonnull
    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists, @Nonnull String temporaryFunctionName) {
        throw reject("DROP TEMPORARY FUNCTION");
    }
}

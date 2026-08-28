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

    private static RuntimeException reject(final String operation) {
        return new RelationalException(
                "DDL operation '" + operation + "' is not allowed in this context",
                ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
    }

    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(SchemaTemplate template, Options templateProperties) {
        throw reject("CREATE SCHEMA TEMPLATE");
    }

    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(String templateId, boolean throwIfDoesNotExist, Options options) {
        throw reject("DROP SCHEMA TEMPLATE");
    }

    @Override
    public ConstantAction getCreateDatabaseConstantAction(URI dbPath, Options constantActionOptions) {
        throw reject("CREATE DATABASE");
    }

    @Override
    public ConstantAction getCreateSchemaConstantAction(URI dbUri, String schemaName, String templateId, Options constantActionOptions) {
        throw reject("CREATE SCHEMA");
    }

    @Override
    public ConstantAction getDropDatabaseConstantAction(URI dbUrl, boolean throwIfDoesNotExist, Options options) {
        throw reject("DROP DATABASE");
    }

    @Override
    public ConstantAction getDropSchemaConstantAction(URI dbPath, String schemaName, Options options) {
        throw reject("DROP SCHEMA");
    }

    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(SchemaTemplate template, boolean throwIfExists, RecordLayerInvokedRoutine invokedRoutine) {
        throw reject("CREATE TEMPORARY FUNCTION");
    }

    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists, String temporaryFunctionName) {
        throw reject("DROP TEMPORARY FUNCTION");
    }
}

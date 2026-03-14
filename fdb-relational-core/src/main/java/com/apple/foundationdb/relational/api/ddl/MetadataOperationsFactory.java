/*
 * MetadataOperationsFactory.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;

@ThreadSafe
public interface MetadataOperationsFactory {

    @Nonnull
    ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties);

    @Nonnull
    ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options);

    @Nonnull
    ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions);

    @Nonnull
    ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions);

    @Nonnull
    ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options);

    @Nonnull
    ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schemaName, @Nonnull Options options);

    @Nonnull
    ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull SchemaTemplate template, boolean throwIfExists, @Nonnull RecordLayerInvokedRoutine invokedRoutine);

    @Nonnull
    ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists, @Nonnull String temporaryFunctionName);
}

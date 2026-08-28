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

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;

@ThreadSafe
public interface MetadataOperationsFactory {

    ConstantAction getSaveSchemaTemplateConstantAction(SchemaTemplate template, Options templateProperties);

    ConstantAction getDropSchemaTemplateConstantAction(String templateId, boolean throwIfDoesNotExist, Options options);

    ConstantAction getCreateDatabaseConstantAction(URI dbPath, Options constantActionOptions);

    ConstantAction getCreateSchemaConstantAction(URI dbUri, String schemaName, String templateId, Options constantActionOptions);

    ConstantAction getDropDatabaseConstantAction(URI dbUrl, boolean throwIfDoesNotExist, Options options);

    ConstantAction getDropSchemaConstantAction(URI dbPath, String schemaName, Options options);

    ConstantAction getCreateTemporaryFunctionConstantAction(SchemaTemplate template, boolean throwIfExists, RecordLayerInvokedRoutine invokedRoutine);

    ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists, String temporaryFunctionName);
}

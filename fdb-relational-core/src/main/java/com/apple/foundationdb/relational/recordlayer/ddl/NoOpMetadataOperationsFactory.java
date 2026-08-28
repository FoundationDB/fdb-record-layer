/*
 * NoOpMetadataOperationsFactory.java
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
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;

import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public final class NoOpMetadataOperationsFactory implements MetadataOperationsFactory {
    public static final NoOpMetadataOperationsFactory INSTANCE = new NoOpMetadataOperationsFactory();

    private NoOpMetadataOperationsFactory() {
    }

    @Override
    public ConstantAction getSaveSchemaTemplateConstantAction(SchemaTemplate templateName, Options templateProperties) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getCreateDatabaseConstantAction(URI dbPath, Options constantActionOptions) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getCreateSchemaConstantAction(URI dbUri, String schemaName, String templateId, Options constantActionOptions) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getDropDatabaseConstantAction(URI dbUrl, boolean throwIfDoesNotExist, Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getDropSchemaConstantAction(URI dbPath, String schemaName, Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getCreateTemporaryFunctionConstantAction(final SchemaTemplate template, boolean throwIfExists,
                                                                   final RecordLayerInvokedRoutine invokedRoutine) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getDropTemporaryFunctionConstantAction(boolean throwIfNotExists,
                                                                 final String temporaryFunctionName) {
        return NoOpConstantAction.INSTANCE;
    }

    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(String templateId, boolean throwIfDoesNotExist, Options options) {
        return NoOpConstantAction.INSTANCE;
    }

    private static class NoOpConstantAction implements ConstantAction {
        private static final NoOpConstantAction INSTANCE = new NoOpConstantAction();

        @Override
        public void execute(Transaction txn) throws RelationalException {
        }
    }
}

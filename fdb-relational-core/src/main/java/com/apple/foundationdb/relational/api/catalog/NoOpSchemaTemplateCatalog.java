/*
 * NoOpSchemaTemplateCatalog.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractRecordLayerResultSet;
import com.apple.foundationdb.relational.transactionbound.catalog.HollowSchemaTemplateCatalog;

import javax.annotation.Nonnull;

/**
 * Implementation of Schema template catalog that ignores CRUD operations on templates. This is essentially used
 * to instantiate a store catalog object that ends up not caring about the schema template in the Schema.
 *
 * Overview of operations:
 *    1. Membership check always return {@code true}
 *    2. List templates return empty {@link RelationalResultSet}
 *    3. Update and delete templates are noop
 */
public class NoOpSchemaTemplateCatalog extends HollowSchemaTemplateCatalog {

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName, long version) {
        return true;
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) {
        return true;
    }

    @Override
    public void updateTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate newTemplate) {
    }

    @Override
    public RelationalResultSet listTemplates(@Nonnull Transaction txn) {
        return new AbstractRecordLayerResultSet(null) {
            @Override
            protected Row advanceRow() {
                return null;
            }

            @Nonnull
            @Override
            public Continuation getContinuation() {
                return Continuation.BEGIN;
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateId) {
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateId, long version) {
    }

}

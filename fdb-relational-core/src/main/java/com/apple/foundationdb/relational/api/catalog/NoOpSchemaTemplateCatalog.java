/*
 * NoOpSchemaTemplateCatalog.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractRecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.metadata.NoOpSchemaTemplate;
import com.apple.foundationdb.relational.transactionbound.catalog.HollowSchemaTemplateCatalog;

import java.util.List;

/**
 * Implementation of Schema template catalog that ignores CRUD operations on templates. This is essentially used
 * to instantiate a store catalog object that ends up not caring about the schema template in the Schema.
 * <p>
 * Overview of operations:
 * <ul>
 *   <li> Membership check always return {@code true} </li>
 *   <li> List templates return empty {@link RelationalResultSet} </li>
 *   <li> Update and delete templates are noop </li>
 *   <li> Loads the schema template for a given (name, version) with a NoOpSchemaTemplate with the (name, version).</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class NoOpSchemaTemplateCatalog extends HollowSchemaTemplateCatalog {

    @Override
    public boolean doesSchemaTemplateExist(Transaction txn, String templateName, int version) {
        return true;
    }

    @Override
    public boolean doesSchemaTemplateExist(Transaction txn, String templateName) {
        return true;
    }

    @Override
    public void createTemplate(Transaction txn, SchemaTemplate newTemplate) {
    }

    @Override
    public SchemaTemplate loadSchemaTemplate(Transaction txn, String templateId, int version) {
        return new NoOpSchemaTemplate(templateId, version);
    }

    @Override
    public RelationalResultSet listTemplates(Transaction txn) {
        // Previously constructed with a null StructMetaData; use a real (empty) one instead so
        // getMetaData() below doesn't have to handle a null metaData field.
        return new AbstractRecordLayerResultSet(RelationalStructMetaData.of(DataType.StructType.from("EMPTY", List.of(), false))) {
            @Override
            protected boolean hasNext() {
                return false;
            }

            @Override
            protected Row advanceRow() {
                // hasNext() is hard-coded false above, so this is never actually called; returning null here
                // would violate advanceRow()'s @NonNull contract, but changing that contract to accommodate
                // this dead branch is out of scope here.
                throw new IllegalStateException("advanceRow() should never be called: hasNext() is always false");
            }

            @Override
            public Continuation getContinuation() {
                return ContinuationImpl.BEGIN;
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        };
    }

    @Override
    public void deleteTemplate(Transaction txn, String templateId, boolean throwIfDoesNotExist) {
    }

    @Override
    public void deleteTemplate(Transaction txn, String templateId, int version, boolean throwIfDoesNotExist) {
    }

}

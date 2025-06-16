/*
 * DropDatabaseConstantAction.java
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
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;

import java.net.URI;
import java.sql.SQLException;

@API(API.Status.EXPERIMENTAL)
public class DropDatabaseConstantAction implements ConstantAction {
    private final URI dbUrl;
    private final boolean throwIfDoesNotExist;
    private final Options options;
    private final StoreCatalog catalog;
    private final MetadataOperationsFactory metadataOperationsFactory;

    public DropDatabaseConstantAction(URI dbUrl,
                                      boolean throwIfDoesNotExist, StoreCatalog catalog,
                                      MetadataOperationsFactory metadataOperationsFactory,
                                      Options options) {
        this.dbUrl = dbUrl;
        this.throwIfDoesNotExist = throwIfDoesNotExist;
        this.options = options;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.catalog = catalog;
    }

    @Override
    public void execute(Transaction txn, final Options options) throws RelationalException {
        if ("/__SYS".equals(dbUrl.getPath())) {
            throw new RelationalException("Cannot drop /__SYS database", ErrorCode.INSUFFICIENT_PRIVILEGE);
        }

        try (RelationalResultSet schemas = catalog.listSchemas(txn, dbUrl, ContinuationImpl.BEGIN)) {
            while (schemas.next()) {
                String schemaName = schemas.getString("SCHEMA_NAME");
                metadataOperationsFactory.getDropSchemaConstantAction(dbUrl, schemaName, this.options).execute(txn, options);
            }
        } catch (SQLException se) {
            ErrorCode ec = ErrorCode.get(se.getSQLState());
            throw new RelationalException(se.getMessage(), ec, se);
        }

        catalog.deleteDatabase(txn, dbUrl, throwIfDoesNotExist);
    }
}

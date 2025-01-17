/*
 * CreateDatabaseConstantAction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;

import javax.annotation.Nonnull;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public class CreateDatabaseConstantAction implements ConstantAction {
    private final URI dbUrl;

    private final StoreCatalog storeCatalog;
    private final KeySpace keySpace;

    public CreateDatabaseConstantAction(@Nonnull URI dbUrl,
                                        @Nonnull StoreCatalog storeCatalog,
                                        @Nonnull KeySpace keySpace) {
        this.dbUrl = dbUrl;
        this.storeCatalog = storeCatalog;
        this.keySpace = keySpace;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        // Probably not the best way to do so, we just need to verify if the database path is valid.
        // TODO (pranjal_gupta2): find a better way to do this validation
        RelationalKeyspaceProvider.toDatabasePath(dbUrl, keySpace);
        //verify that the database doesn't exist already
        if (storeCatalog.doesDatabaseExist(txn, dbUrl)) {
            throw new RelationalException("Database " + dbUrl + " already exists", ErrorCode.DATABASE_ALREADY_EXISTS);
        }
        storeCatalog.createDatabase(txn, dbUrl);
    }
}

/*
 * CreateDatabaseConstantAction.java
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;

import java.net.URI;
import java.util.Map;

public class CreateDatabaseConstantAction implements ConstantAction{
    private final URI dbUrl;
    private final DatabaseTemplate dbTemplate;
    private final Options constantActionOptions;

    private final ConstantActionFactory caFactory;
    private final KeySpace keySpace;
    private final URI templateBasePath;

    public CreateDatabaseConstantAction(URI dbUrl, DatabaseTemplate dbTemplate, Options constantActionOptions, ConstantActionFactory caFactory, KeySpace keySpace, URI templateBasePath) {
        this.dbUrl = dbUrl;
        this.dbTemplate = dbTemplate;
        this.constantActionOptions = constantActionOptions;
        this.caFactory = caFactory;
        this.keySpace = keySpace;
        this.templateBasePath = templateBasePath;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        //TODO(bfines) catch errors here
        KeySpacePath dbPath = KeySpaceUtils.uriToPath(dbUrl,keySpace);

        final KeySpaceDirectory dbDirectory = dbPath.getDirectory();

        for(Map.Entry<String,String> schemaData : dbTemplate.getSchemaToTemplateNameMap().entrySet()){

            URI templateUri =  URI.create(templateBasePath.toString()+"/"+schemaData.getValue());

            caFactory.getCreateSchemaConstantAction(dbUrl, schemaData.getKey(), templateUri, constantActionOptions).execute(txn);
        }

    }
}

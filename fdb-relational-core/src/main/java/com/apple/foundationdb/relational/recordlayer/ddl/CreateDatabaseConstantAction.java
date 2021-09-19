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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;

import java.net.URI;
import java.util.Map;

public class CreateDatabaseConstantAction implements ConstantAction {
    private final URI dbUrl;
    private final DatabaseTemplate dbTemplate;
    private final Options constantActionOptions;

    private final ConstantActionFactory caFactory;

    public CreateDatabaseConstantAction(URI dbUrl, DatabaseTemplate dbTemplate, Options constantActionOptions, ConstantActionFactory caFactory) {
        this.dbUrl = dbUrl;
        this.dbTemplate = dbTemplate;
        this.constantActionOptions = constantActionOptions;
        this.caFactory = caFactory;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        for (Map.Entry<String, String> schemaData : dbTemplate.getSchemaToTemplateNameMap().entrySet()) {
            URI schemaUrl = URI.create(KeySpaceUtils.getPath(dbUrl) + "/" + schemaData.getKey());
            caFactory.getCreateSchemaConstantAction(schemaUrl, schemaData.getValue(), constantActionOptions).execute(txn);
        }
    }
}

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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DropDatabaseConstantAction implements ConstantAction {
    private final URI dbUrl;
    private final KeySpace baseKeySpace;
    private final Options options;
    private final RecordLayerConstantActionFactory constantActionFactory;

    public DropDatabaseConstantAction(URI dbUrl, KeySpace baseKeySpace, Options options, RecordLayerConstantActionFactory constantActionFactory) {
        this.dbUrl = dbUrl;
        this.baseKeySpace = baseKeySpace;
        this.options = options;
        this.constantActionFactory = constantActionFactory;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        KeySpacePath path = KeySpaceUtils.uriToPath(dbUrl,baseKeySpace);
        KeySpaceDirectory dir = path.getDirectory();
        //each subdirectory is a schema, so drop each one individually

        //make a copy to avoid a CME
        final List<KeySpaceDirectory> subdirectories = new ArrayList<>(dir.getSubdirectories());
        for(KeySpaceDirectory schemaDir : subdirectories){
            String schemaName = schemaDir.getName();
            if(!schemaName.startsWith("/"))
                schemaName = "/"+schemaName;
            URI schemaUrl = URI.create(dbUrl + schemaName);
            constantActionFactory.getDropSchemaConstantAction(schemaUrl,options).execute(txn);
        }
    }
}

/*
 * CentralStoreConfiguration.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.centralstore;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;

import java.util.ArrayList;
import java.util.List;

public class CentralStoreConfiguration {
    private static final String DEFAULT_STORE_NAME = "central_store";

    KeySpacePath storePath = new KeySpace(new KeySpaceDirectory(DEFAULT_STORE_NAME, KeySpaceDirectory.KeyType.NULL))
            .path(DEFAULT_STORE_NAME);
    int metadataVersion = -1;
    List<CentralStoreType> types = new ArrayList<>();

    CentralStoreConfiguration() { }

    public CentralStoreConfiguration setStorePath(KeySpacePath storePath) {
        this.storePath = storePath;
        return this;
    }

    public CentralStoreConfiguration setMetadataVersion(int metadataVersion) {
        this.metadataVersion = metadataVersion;
        return this;
    }

    // The added type should not have same message name as existing ones. But we don't check it here. It should be
    // caught by MetaDataProtoEditor.addRecordType
    public CentralStoreConfiguration addType(CentralStoreType type) {
        types.add(type);
        return this;
    }

    public void configure() {
        if (metadataVersion < 0) {
            throw new RecordCoreException("Metadata version should be set in CentralStoreConfiguration");
        }
        CentralStore.configure(this);
    }
}

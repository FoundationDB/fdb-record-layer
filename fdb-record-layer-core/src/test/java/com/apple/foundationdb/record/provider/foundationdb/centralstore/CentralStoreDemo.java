/*
 * CentralStoreDemo.java
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

import com.apple.foundationdb.record.CentralStoreProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

// TODO: The actual tests should test the general infrastructure of CentralStore and the underlying types separately.
//  This only serves as a demonstration of how the Central Store can be configured and used.
public class CentralStoreDemo extends FDBTestBase {
    FDBDatabase fdb;

    @BeforeEach
    public void setup() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
    }

    @Test
    public void demo() {
        final int INITIAL_META_DATA_VERSION = 0;
        CentralStore.newConfiguration()
                .setStorePath(TestKeySpace.getKeyspacePath("record-test", "unit", "centralStore"))
                .setMetadataVersion(INITIAL_META_DATA_VERSION)
                .addType(CentralStoreTypeStoreInfo.config(null, Collections.singletonList(new Index("testIndex", "metadata_version"))))
                .configure();

        final Subspace testSubspace = new Subspace(Tuple.from("testSubspace"));
        fdb.run(context -> CentralStoreTypeStoreInfo.runIfEnabled(context,
                testSubspace,
                CentralStoreTypeStoreInfo.InfoType.METADATA_VERSION,
                store -> store.saveRecord(CentralStoreProto.StoreInfo.newBuilder()
                        .setStoreSubspace(ByteString.copyFrom(testSubspace.pack()))
                        .setMetadataVersion(2)
                        .build())
        ));
    }
}

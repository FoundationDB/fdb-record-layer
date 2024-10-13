/*
 * AbstractSyntheticRecordPlannerTest.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Base class for {@link SyntheticRecordPlanner} tests.
 */
public abstract class AbstractSyntheticRecordPlannerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    protected FDBDatabase fdb;
    protected FDBStoreTimer timer;
    protected RecordMetaDataBuilder metaDataBuilder;
    protected FDBRecordStore.Builder recordStoreBuilder;

    protected FDBRecordContext openContext() {
        return fdb.openContext(null, timer);
    }

    @BeforeEach
    public void initBuilders() {
        metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());

        fdb = dbExtension.getDatabase();
        KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        recordStoreBuilder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setKeySpacePath(path);
        timer = new FDBStoreTimer();
    }
}

/*
 * ValueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Tests for the {@link ValueIndexMaintainer} using the shared index scenario framework.
 */
@Tag(Tags.RequiresFDB)
public class ValueIndexTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        path = pathManager.createPath();
    }

    @ParameterizedTest
    @IndexScenarios
    void indexScenariosTest(IndexScenario scenario) throws Exception {
        scenario.runTest(
                () -> new ValueIndexDefinition(),
                () -> {
                    FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                            .setTimer(new FDBStoreTimer())
                            .build();
                    return dbExtension.getDatabase().openContext(config);
                },
                FDBRecordStore.newBuilder()
                        .setKeySpacePath(path));
    }
}

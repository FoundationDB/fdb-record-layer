/*
 * AtomicMutationIndexTest.java
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

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenariosArgumentsProvider;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@Tag(Tags.RequiresFDB)
public class AtomicMutationIndexTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        path = pathManager.createPath();
    }

    public static Stream<Arguments> indexScenariosTest() {
        return ParameterizedTestUtils.cartesianProduct(
                IndexScenariosArgumentsProvider.getScenarios(),
                Stream.of(
                        IndexTypes.COUNT,
                        IndexTypes.COUNT_UPDATES,
                        IndexTypes.COUNT_NOT_NULL,
                        IndexTypes.SUM,
                        IndexTypes.MIN_EVER_LONG,
                        IndexTypes.MAX_EVER_LONG
                )
        );
    }

    @ParameterizedTest
    @MethodSource
    void indexScenariosTest(IndexScenario scenario, String indexType) throws Exception {
        scenario.runTest((groupingLength, syntheticType) -> new AtomicMutationIndexDefinition(indexType),
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

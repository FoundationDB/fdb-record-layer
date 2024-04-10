/*
 * TestKeySpace.java
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

package com.apple.foundationdb.record.test;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A KeySpace and some helper methods for testing.
 */
public class TestKeySpace {

    private static final Logger LOG = LogManager.getLogger(TestKeySpace.class);

    public static final String TEST_UUID = "testUuid";
    public static final String RECORD_STORE = "recordStore";
    public static final String META_DATA_STORE = "metaDataStore";
    public static final String RAW_DATA = "rawData";
    public static final String MULTI_RECORD_STORE = "multiRecordStore";
    public static final String STORE_PATH = "storePath";
    public static final String RESOLVER_MAPPING_REPLICATOR = "resolverMappingReplicator";
    public static final String RESOLVER_HOOKS = "resolverHooks";

    public static final KeySpace keySpace = new KeySpace(
            new DirectoryLayerDirectory("record-test", "record-test")
                    .addSubdirectory(new DirectoryLayerDirectory("unit", "unit")
                            .addSubdirectory(new KeySpaceDirectory(TEST_UUID, KeySpaceDirectory.KeyType.STRING)
                                    .addSubdirectory(new DirectoryLayerDirectory(RECORD_STORE, RECORD_STORE))
                                    .addSubdirectory(new DirectoryLayerDirectory(META_DATA_STORE, META_DATA_STORE))
                                    .addSubdirectory(new DirectoryLayerDirectory(RAW_DATA, RAW_DATA))
                                    .addSubdirectory(new DirectoryLayerDirectory(MULTI_RECORD_STORE, MULTI_RECORD_STORE)
                                            .addSubdirectory(new DirectoryLayerDirectory(STORE_PATH))
                                    )
                                    .addSubdirectory(new DirectoryLayerDirectory(RESOLVER_MAPPING_REPLICATOR, RESOLVER_MAPPING_REPLICATOR)
                                            .addSubdirectory(new KeySpaceDirectory("to", KeySpaceDirectory.KeyType.STRING, "to")
                                                    .addSubdirectory(new KeySpaceDirectory("primary", KeySpaceDirectory.KeyType.STRING, "primary"))
                                                    .addSubdirectory(new KeySpaceDirectory("replica", KeySpaceDirectory.KeyType.STRING, "replica"))
                                            )
                                    )
                                    .addSubdirectory(new DirectoryLayerDirectory(RESOLVER_HOOKS, RESOLVER_HOOKS)
                                            .addSubdirectory(new KeySpaceDirectory("resolvers", KeySpaceDirectory.KeyType.STRING, "resolvers")
                                                    .addSubdirectory(new KeySpaceDirectory("resolverNode", KeySpaceDirectory.KeyType.STRING)))
                                            .addSubdirectory(new KeySpaceDirectory("should-use-A", KeySpaceDirectory.KeyType.STRING, "should-use-A"))
                                    )
                            )
                    )
                    .addSubdirectory(new DirectoryLayerDirectory("performance", "performance")
                            .addSubdirectory(new DirectoryLayerDirectory("recordStore", "recordStore"))
                            .addSubdirectory(new DirectoryLayerDirectory("luceneScaleTest", "luceneScaleTest")
                                    .addSubdirectory(new KeySpaceDirectory("run", KeySpaceDirectory.KeyType.STRING)))
                    )
    );

    /**
     * A method that roughly approximates mapPathKeys, designed only for use in test suite setup. The provided path
     * must be valid in the test keys space.
     */
    public static KeySpacePath getKeyspacePath(Object... pathElements) {
        try {
            if (pathElements.length <= 0) {
                fail("must call this method with at least one path element");
            }
            KeySpacePath path = keySpace.path((String)pathElements[0]);
            for (int i = 1; i < pathElements.length; i++) {
                path = path.add((String)pathElements[i]);
            }
            return path;
        } catch (NoSuchDirectoryException e) {
            LOG.error(KeyValueLogMessage.build("Failed to get TestKeySpace")
                    .addKeysAndValues(e.getLogInfo())
                    .toString());
            throw e;
        }
    }
}

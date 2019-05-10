/*
 * TestKeySpace.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A KeySpace and some helper methods for testing.
 */
public class TestKeySpace {
    public static final KeySpace keySpace = new KeySpace(
            new DirectoryLayerDirectory("record-test", "record-test")
                    .addSubdirectory(new DirectoryLayerDirectory("unit", "unit")
                            .addSubdirectory(new DirectoryLayerDirectory("recordStore", "recordStore"))
                            .addSubdirectory(new DirectoryLayerDirectory("corruptRecordStore", "corruptRecordStore"))
                            .addSubdirectory(new DirectoryLayerDirectory("multiRecordStore", "multiRecordStore")
                                    .addSubdirectory(new DirectoryLayerDirectory("storePath"))
                            )
                            .addSubdirectory(new DirectoryLayerDirectory("metadataStore", "metadataStore"))
                            .addSubdirectory(new DirectoryLayerDirectory("keyvaluecursor", "keyvaluecursor"))
                            .addSubdirectory(new DirectoryLayerDirectory("ackeyvaluecursor", "ackeyvaluecursor"))
                            .addSubdirectory(new DirectoryLayerDirectory("typedtest", "typedtest"))
                            .addSubdirectory(new DirectoryLayerDirectory("concatcursor", "concatcursor"))
                            .addSubdirectory(new DirectoryLayerDirectory("indexTest", "indexTest")
                                    .addSubdirectory(new KeySpaceDirectory("leaderboard", KeySpaceDirectory.KeyType.LONG, 8L))
                                    .addSubdirectory(new KeySpaceDirectory("version", KeySpaceDirectory.KeyType.LONG, 9L))
                            )
                    )
                    .addSubdirectory(new DirectoryLayerDirectory("performance", "performance")
                            .addSubdirectory(new DirectoryLayerDirectory("recordStore", "recordStore"))
                    )
    );

    /**
     * A method that roughly approximates mapPathKeys, designed only for use in test suite setup. The provided path
     * must be valid in the test keys space.
     */
    public static KeySpacePath getKeyspacePath(Object... pathElements) {
        if (pathElements.length <= 0) {
            fail("must call this method with at least one path element");
        }
        KeySpacePath path = keySpace.path((String) pathElements[0]);
        for (int i = 1; i < pathElements.length; i++) {
            path = path.add((String) pathElements[i]);
        }
        return path;
    }
}

/*
 * FakeClusterFileUtil.java
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

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Utility class for generating fake cluster files. These do not contain cluster files for working FDB
 * clusters, but they can be used for a few tests which test how things operate when the database is
 * inaccessible.
 */
public class FakeClusterFileUtil {
    private FakeClusterFileUtil() {
    }

    @Nonnull
    public static String createFakeClusterFile(String prefix) throws IOException {
        File clusterFile = File.createTempFile(prefix, ".cluster");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(clusterFile))) {
            writer.write("fake:fdbcluster@127.0.0.1:65535\n");
        }
        return clusterFile.getAbsolutePath();
    }
}

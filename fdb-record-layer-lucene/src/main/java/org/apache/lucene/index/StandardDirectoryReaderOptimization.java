/*
 * StandardDirectoryReaderOptimization.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An optimization to open the segment readers in parallel when opening a directory.
 *
 */
public class StandardDirectoryReaderOptimization {

    private StandardDirectoryReaderOptimization() {
    }

    /** called from DirectoryReader.open(...) methods */
    public static DirectoryReader open(
            final Directory directory, final IndexCommit commit, Comparator<LeafReader> leafSorter, Executor executor) throws IOException {
        return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
            @Override
            protected DirectoryReader doBody(String segmentFileName) throws IOException {
                SegmentInfos sis = SegmentInfos.readCommit(directory, segmentFileName);
                final SegmentReader[] readers = new SegmentReader[sis.size()];
                boolean success = false;
                try {
                    List<CompletableFuture<SegmentReader>>  futures = new ArrayList<>(sis.size());
                    for (int i = sis.size() - 1; i >= 0; i--) {
                        final SegmentCommitInfo info = sis.info(i);
                        futures.add(CompletableFuture.supplyAsync( () -> {
                            try {
                                return new SegmentReader(info, sis.getIndexCreatedVersionMajor(), IOContext.READ);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor));
                    }
                    int j = 0;
                    for (int i = sis.size() - 1; i >= 0; i--) {
                        readers[i] = futures.get(j).join();
                        j++;
                    }
                    // This may throw CorruptIndexException if there are too many docs, so
                    // it must be inside try clause so we close readers in that case:
                    DirectoryReader reader = new StandardDirectoryReader(directory, readers, null, sis, leafSorter, false, false);
                    success = true;

                    return reader;
                } finally {
                    if (!success) {
                        IOUtils.closeWhileHandlingException(readers);
                    }
                }
            }
        }.run(commit);
    }
}

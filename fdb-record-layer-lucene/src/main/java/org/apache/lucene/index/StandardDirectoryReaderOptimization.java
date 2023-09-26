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
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * An optimization to open the segment readers in parallel when opening a directory.
 *
 */
public class StandardDirectoryReaderOptimization {

    private StandardDirectoryReaderOptimization() {
    }

    /**
     * Called from {@link DirectoryReader#open} methods.
     * @param directory the directory to open
     * @param commit the index changes to open
     * @param leafSorter a comparator for sorting leaf readers
     * @param executor executor to use to run in parallel
     * @param parallelism number of readers to open in parallel
     * @return an open directory reader
     * @throws IOException if there is a problem with the underlying implementation
     */
    public static DirectoryReader open(
            final Directory directory, final IndexCommit commit, final Comparator<LeafReader> leafSorter,
            final Executor executor, final int parallelism) throws IOException {
        return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
            @Override
            protected DirectoryReader doBody(String segmentFileName) throws IOException {
                SegmentInfos sis = SegmentInfos.readCommit(directory, segmentFileName);
                final SegmentReader[] readers = new SegmentReader[sis.size()];
                boolean success = false;
                try {
                    final Deque<SegmentReaderOpener> openers = new ArrayDeque<>(parallelism);
                    int readerIndex = 0;
                    while (true) {
                        while (readerIndex < readers.length && openers.size() < parallelism) {
                            final SegmentCommitInfo info = sis.info(sis.size() - 1 - readerIndex);
                            final SegmentReaderOpener opener = new SegmentReaderOpener(readers, readerIndex,
                                    info, sis.getIndexCreatedVersionMajor(), IOContext.READ);
                            readerIndex++;
                            openers.add(opener);
                            opener.start(executor);
                        }
                        if (openers.isEmpty()) {
                            break;
                        }
                        openers.pop().finish();
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

    static class SegmentReaderOpener {
        private final SegmentReader[] segmentReaders;
        private final int readerIndex;

        private final SegmentCommitInfo segmentCommitInfo;
        private final int createdVersionMajor;
        private final IOContext ioContext;

        private IOException ioException;
        private CompletableFuture<Void> future;

        SegmentReaderOpener(final SegmentReader[] segmentReaders, final int readerIndex,
                            final SegmentCommitInfo segmentCommitInfo, final int createdVersionMajor, final IOContext ioContext) {
            this.segmentReaders = segmentReaders;
            this.readerIndex = readerIndex;
            this.segmentCommitInfo = segmentCommitInfo;
            this.createdVersionMajor = createdVersionMajor;
            this.ioContext = ioContext;
        }

        public void start(final Executor executor) {
            try {
                future = CompletableFuture.supplyAsync(() -> {
                    try {
                        open();
                    } catch (IOException ex) {
                        ioException = ex;
                    } catch (RejectedExecutionException ex) {
                        // This happens when trying to block in openSchema and having run out of extra threads.
                        segmentReaders[readerIndex] = null;
                    }
                    return null;
                }, executor);
            } catch (RejectedExecutionException ex) {
                future = null;
            }
        }

        @SuppressWarnings("PMD.PreserveStackTrace")
        public void finish() throws IOException {
            if (future != null) {
                future.join();
            }
            if (ioException != null) {
                throw ioException;
            }
            if (segmentReaders[readerIndex] == null) {
                open();
            }
        }

        private void open() throws IOException {
            segmentReaders[readerIndex] = new SegmentReader(segmentCommitInfo, createdVersionMajor, ioContext);
        }
    }
}

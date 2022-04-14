/*
 * FDBDirectoryWrapper.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCodec;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedWrappedBlendedInfixSuggester;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Wrapper containing an {@link FDBDirectory} and cached accessor objects (like {@link IndexWriter}s). This object
 * is designed to be held by the {@link FDBDirectoryManager}, with one object cached per open directory. Because the
 * {@link FDBDirectory} contains cached information from FDB, it is important for cache coherency that all writers
 * (etc.) accessing that directory go through the same wrapper object so that they share a common cache.
 */
class FDBDirectoryWrapper implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryWrapper.class);

    private final IndexMaintainerState state;
    private final FDBDirectory directory;
    @SuppressWarnings({"squid:S3077"}) // object is thread safe, so use of volatile to control instance creation is correct
    private volatile IndexWriter writer;
    @SuppressWarnings({"squid:S3077"}) // object is thread safe, so use of volatile to control instance creation is correct
    private volatile AnalyzingInfixSuggester suggester;

    FDBDirectoryWrapper(IndexMaintainerState state, FDBDirectory directory) {
        this.state = state;
        this.directory = directory;
    }

    public FDBDirectory getDirectory() {
        return directory;
    }

    public IndexReader getReader() throws IOException {
        IndexWriter indexWriter = writer;
        if (writer == null) {
            return DirectoryReader.open(directory);
        } else {
            return DirectoryReader.open(indexWriter);
        }
    }

    @Nonnull
    public IndexWriter getWriter(Analyzer analyzer) throws IOException {
        if (writer == null) {
            synchronized (this) {
                if (writer == null) {
                    TieredMergePolicy tieredMergePolicy = new TieredMergePolicy()
                            .setMaxMergedSegmentMB(Math.max(0.0, state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE)));
                    tieredMergePolicy.setNoCFSRatio(1.00);
                    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer)
                            .setUseCompoundFile(true)
                            .setMergePolicy(tieredMergePolicy)
                            .setMergeScheduler(new ConcurrentMergeScheduler() {
                                @Override
                                public synchronized void merge(final MergeSource mergeSource, final MergeTrigger trigger) throws IOException {
                                    LOGGER.trace("mergeSource={}", mergeSource);
                                    super.merge(mergeSource, trigger);
                                }
                            })
                            .setCodec(new LuceneOptimizedCodec())
                            .setInfoStream(new LuceneLoggerInfoStream(LOGGER));
                    writer = new IndexWriter(directory, indexWriterConfig);
                }
            }
        }
        return writer;
    }

    @Nonnull
    public AnalyzingInfixSuggester getAutocompleteSuggester(@Nonnull Analyzer indexAnalyzer,
                                                            @Nonnull Analyzer queryAnalyzer,
                                                            boolean highlight) {
        if (suggester == null) {
            synchronized (this) {
                if (suggester == null) {
                    suggester = LuceneOptimizedWrappedBlendedInfixSuggester.getSuggester(state, directory, indexAnalyzer, queryAnalyzer, highlight);
                }
            }
        }
        return suggester;
    }

    @Override
    public synchronized void close() throws IOException {
        IndexWriter indexWriter = writer;
        if (indexWriter != null) {
            indexWriter.close();
            writer = null;
        }
        if (suggester != null) {
            suggester.close();
            suggester = null;
        }
        directory.close();
    }
}

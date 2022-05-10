/*
 * LuceneOptimizedWrappedBlendedInfixSuggester.java
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.BlendedInfixSuggester;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Optimized suggester based on {@link BlendedInfixSuggester} to override the {@link IndexWriterConfig} for index writer.
 */
public class LuceneOptimizedWrappedBlendedInfixSuggester extends BlendedInfixSuggester {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOptimizedWrappedBlendedInfixSuggester.class);

    private static final BlenderType DEFAULT_BLENDER_TYPE = BlenderType.POSITION_LINEAR;

    @Nonnull
    private final IndexMaintainerState state;

    @SuppressWarnings("squid:S107")
    private LuceneOptimizedWrappedBlendedInfixSuggester(@Nonnull IndexMaintainerState state, @Nonnull Directory dir, @Nonnull Analyzer indexAnalyzer,
                                                        @Nonnull Analyzer queryAnalyzer, int minPrefixChars, BlenderType blenderType, int numFactor,
                                                        @Nullable Double exponent, boolean highlight) throws IOException {
        super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, blenderType, numFactor, exponent, false, true, highlight);
        this.state = state;
    }

    @Override
    protected IndexWriterConfig getIndexWriterConfig(Analyzer indexAnalyzer, IndexWriterConfig.OpenMode openMode) {
        TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
        tieredMergePolicy.setMaxMergedSegmentMB(Math.max(0.0, state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_MERGE_MAX_SIZE)));
        tieredMergePolicy.setNoCFSRatio(1.00);
        IndexWriterConfig iwc = super.getIndexWriterConfig(indexAnalyzer, openMode);
        iwc.setUseCompoundFile(true);
        iwc.setMergePolicy(tieredMergePolicy);
        iwc.setMergeScheduler(new ConcurrentMergeScheduler() {

            @Override
            public synchronized void merge(final MergeSource mergeSource, final MergeTrigger trigger) throws IOException {
                if (state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.MERGE_OPTIMIZATION) && trigger == MergeTrigger.FULL_FLUSH) {
                    final String currentLock = state.context.luceneMergeLock.compareAndExchange("available", state.index.getName() + "_auto");
                    if (currentLock.equals("available") || currentLock.equals(state.index.getName() + "_auto")) {
                        String logMsg = KeyValueLogMessage.of("Auto-complete index mergeSource with checking lock",
                                "current_lock", currentLock,
                                LuceneLogMessageKeys.MERGE_SOURCE, mergeSource,
                                LuceneLogMessageKeys.MERGE_TRIGGER, trigger,
                                LuceneLogMessageKeys.INDEX_NAME, state.index.getName(),
                                LuceneLogMessageKeys.INDEX_SUBSPACE, state.indexSubspace);
                        LOGGER.trace(logMsg);
                        super.merge(mergeSource, trigger);
                    } else {
                        String logMsg = KeyValueLogMessage.of("Auto-complete merge aborted due to CAS lock acquired by other directory: " + currentLock,
                                LuceneLogMessageKeys.MERGE_SOURCE, mergeSource,
                                LuceneLogMessageKeys.MERGE_TRIGGER, trigger,
                                LuceneLogMessageKeys.INDEX_NAME, state.index.getName(),
                                LuceneLogMessageKeys.INDEX_SUBSPACE, state.indexSubspace);
                        LOGGER.trace(logMsg);
                        synchronized (mergeSource) {
                            MergePolicy.OneMerge nextMerge = mergeSource.getNextMerge();
                            while (nextMerge != null) {
                                nextMerge.setAborted();
                                mergeSource.onMergeFinished(nextMerge);
                                nextMerge = mergeSource.getNextMerge();
                            }
                        }
                    }
                } else {
                    String logMsg = KeyValueLogMessage.of("Auto-complete index mergeSourcee",
                            LuceneLogMessageKeys.MERGE_SOURCE, mergeSource,
                            LuceneLogMessageKeys.MERGE_TRIGGER, trigger,
                            LuceneLogMessageKeys.INDEX_NAME, state.index.getName(),
                            LuceneLogMessageKeys.INDEX_SUBSPACE, state.indexSubspace);
                    LOGGER.trace(logMsg);
                    super.merge(mergeSource, trigger);
                    return;
                }
            }
        });
        iwc.setCodec(new LuceneOptimizedCodec());
        iwc.setInfoStream(new LuceneLoggerInfoStream(LOGGER));
        return iwc;
    }

    @Nonnull
    public static AnalyzingInfixSuggester getSuggester(@Nonnull IndexMaintainerState state, @Nonnull Directory dir,
                                                       @Nonnull Analyzer indexAnalyzer, @Nonnull Analyzer queryAnalyzer,
                                                       boolean highlight, @Nonnull IndexOptions indexOptions) {
        final String autoCompleteBlenderType = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_TYPE);
        final String autoCompleteBlenderNumFactor = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_NUM_FACTOR);
        final String autoCompleteMinPrefixSize = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE);
        final String autoCompleteBlenderExponent = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_EXPONENT);

        final boolean useTermVectors = state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_WITH_TERM_VECTORS);
        final int minPrefixChars = autoCompleteMinPrefixSize == null ? DEFAULT_MIN_PREFIX_CHARS : Integer.parseInt(autoCompleteMinPrefixSize);
        final BlenderType blenderType = autoCompleteBlenderType == null ? DEFAULT_BLENDER_TYPE : BlenderType.valueOf(autoCompleteBlenderType);
        final int numFactor = autoCompleteBlenderNumFactor == null ? DEFAULT_NUM_FACTOR : Integer.parseInt(autoCompleteBlenderNumFactor);
        final Double exponent = autoCompleteBlenderExponent == null ? null : Double.valueOf(autoCompleteBlenderExponent);

        try {
            return useTermVectors
                   ? new LuceneOptimizedWrappedBlendedInfixSuggester(state, dir, indexAnalyzer, queryAnalyzer, minPrefixChars, blenderType, numFactor, exponent, highlight)
                   : new LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors(state, dir, indexAnalyzer, queryAnalyzer, minPrefixChars, blenderType,
                    numFactor, exponent, highlight, indexOptions);
        } catch (IllegalArgumentException iae) {
            throw new RecordCoreArgumentException("Invalid parameter for auto complete suggester", iae)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        } catch (IOException ioe) {
            throw new RecordCoreArgumentException("Failure with underlying lucene index opening for auto complete suggester", ioe)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        }
    }

    @Nonnull
    public long deleteDocuments(Query... queries) throws IOException {
        return writer.deleteDocuments(queries);
    }
}

/*
 * LuceneOptimizedWrappedAnalyzingInfixSuggester.java
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.BlendedInfixSuggester;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Optimized suggester to override the codec for index writer.
 */
public class LuceneOptimizedWrappedAnalyzingInfixSuggester extends BlendedInfixSuggester {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOptimizedWrappedAnalyzingInfixSuggester.class);

    private static final BlenderType DEFAULT_BLENDER_TYPE = BlenderType.POSITION_LINEAR;

    @Nonnull
    private final IndexMaintainerState state;

    @SuppressWarnings("squid:S107")
    private LuceneOptimizedWrappedAnalyzingInfixSuggester(@Nonnull IndexMaintainerState state, @Nonnull Directory dir, @Nonnull Analyzer indexAnalyzer,
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
                LOGGER.trace("Auto-complete index mergeSource={}", mergeSource);
                super.merge(mergeSource, trigger);
            }
        });
        iwc.setCodec(new LuceneOptimizedCodec());
        iwc.setInfoStream(new LuceneLoggerInfoStream(LOGGER));
        return iwc;
    }

    @Nonnull
    public static AnalyzingInfixSuggester getSuggester(@Nonnull IndexMaintainerState state, @Nonnull Directory dir, @Nonnull Analyzer indexAnalyzer, @Nonnull Analyzer queryAnalyzer, boolean highlight) {
        String autoCompleteBlenderType = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_TYPE);
        String autoCompleteBlenderNumFactor = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_NUM_FACTOR);
        String autoCompleteMinPrefixSize = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE);
        String autoCompleteBlenderExponent = state.index.getOption(LuceneIndexOptions.AUTO_COMPLETE_BLENDER_EXPONENT);

        try {
            return new LuceneOptimizedWrappedAnalyzingInfixSuggester(state, dir, indexAnalyzer, queryAnalyzer,
                    autoCompleteMinPrefixSize == null ? DEFAULT_MIN_PREFIX_CHARS : Integer.parseInt(autoCompleteMinPrefixSize),
                    autoCompleteBlenderType == null ? DEFAULT_BLENDER_TYPE : BlenderType.valueOf(autoCompleteBlenderType),
                    autoCompleteBlenderNumFactor == null ? DEFAULT_NUM_FACTOR : Integer.parseInt(autoCompleteBlenderNumFactor),
                    autoCompleteBlenderExponent == null ? null : Double.valueOf(autoCompleteBlenderExponent),
                    highlight);
        } catch (IllegalArgumentException iae) {
            throw new RecordCoreArgumentException("Invalid parameter for auto complete suggester", iae)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        } catch (IOException ioe) {
            throw new RecordCoreArgumentException("Failure with underlying lucene index opening for auto complete suggester", ioe)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        }
    }
}

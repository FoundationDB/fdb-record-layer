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
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
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

    private LuceneOptimizedWrappedAnalyzingInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer,
                                                          int minPrefixChars, BlenderType blenderType, int numFactor,
                                                          @Nullable Double exponent, boolean highlight) throws IOException {
        super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, blenderType, numFactor, exponent, false, true, highlight);
    }

    @Override
    protected IndexWriterConfig getIndexWriterConfig(Analyzer indexAnalyzer, IndexWriterConfig.OpenMode openMode) {
        TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
        tieredMergePolicy.setMaxMergedSegmentMB(5.00);
        tieredMergePolicy.setMaxMergeAtOnceExplicit(2);
        tieredMergePolicy.setNoCFSRatio(1.00);
        IndexWriterConfig iwc = super.getIndexWriterConfig(indexAnalyzer, openMode);
        iwc.setUseCompoundFile(true);
        iwc.setMergePolicy(tieredMergePolicy);
        iwc.setMergeScheduler(new ConcurrentMergeScheduler() {
            @Override
            protected void doMerge(final IndexWriter writer, final MergePolicy.OneMerge merge) throws IOException {
                merge.segments.forEach( (segmentCommitInfo) -> LOGGER.trace("Auto-complete index segmentInfo={}", segmentCommitInfo.info.name));
                super.doMerge(writer, merge);
            }
        });
        iwc.setCodec(new LuceneOptimizedCodec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
        iwc.setInfoStream(new LuceneLoggerInfoStream(LOGGER));
        return iwc;
    }

    @Nonnull
    public static AnalyzingInfixSuggester getSuggester(final Index index, @Nonnull Directory dir, @Nonnull Analyzer indexAnalyzer, @Nonnull Analyzer queryAnalyzer, boolean highlight) {
        String autoCompleteBlenderType = index.getOption(IndexOptions.AUTO_COMPLETE_BLENDER_TYPE);
        String autoCompleteBlenderNumFactor = index.getOption(IndexOptions.AUTO_COMPLETE_BLENDER_NUM_FACTOR);
        String autoCompleteMinPrefixSize = index.getOption(IndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE);
        String autoCompleteBlenderExponent = index.getOption(IndexOptions.AUTO_COMPLETE_BLENDER_EXPONENT);

        try {
            return new LuceneOptimizedWrappedAnalyzingInfixSuggester(dir, indexAnalyzer, queryAnalyzer,
                    autoCompleteMinPrefixSize == null ? DEFAULT_MIN_PREFIX_CHARS : Integer.parseInt(autoCompleteMinPrefixSize),
                    autoCompleteBlenderType == null ? DEFAULT_BLENDER_TYPE : BlenderType.valueOf(autoCompleteBlenderType),
                    autoCompleteBlenderNumFactor == null ? DEFAULT_NUM_FACTOR : Integer.parseInt(autoCompleteBlenderNumFactor),
                    autoCompleteBlenderExponent == null ? null : Double.valueOf(autoCompleteBlenderExponent),
                    highlight);
        } catch (IllegalArgumentException iae) {
            throw new RecordCoreArgumentException("Invalid parameter for auto complete suggester", iae)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName());
        } catch (IOException ioe) {
            throw new RecordCoreArgumentException("Failure with underlying lucene index opening for auto complete suggester", ioe)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName());
        }
    }
}

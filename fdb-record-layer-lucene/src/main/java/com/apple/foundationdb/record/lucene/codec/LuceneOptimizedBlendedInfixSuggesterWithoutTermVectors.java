/*
 * LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors.java
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
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.BlendedInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Optimized {@link BlendedInfixSuggester} that does not rely on term vectors persisted in DB.
 */
public class LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors extends AnalyzingInfixSuggester {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors.class);
    private static final Comparator<LookupResult> LOOKUP_COMP = new LookUpComparator();
    private static final double LINEAR_COEF = 0.10;

    @Nonnull
    private final IndexMaintainerState state;
    @Nonnull
    private final BlendedInfixSuggester.BlenderType blenderType;
    @Nonnull
    private final IndexOptions indexOptions;
    private final int numFactor;

    private Double exponent = 2.0;

    @SuppressWarnings("squid:S107")
    LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors(@Nonnull IndexMaintainerState state, @Nonnull Directory dir, @Nonnull Analyzer indexAnalyzer,
                                                           @Nonnull Analyzer queryAnalyzer, int minPrefixChars, BlendedInfixSuggester.BlenderType blenderType, int numFactor,
                                                           @Nullable Double exponent, boolean highlight, @Nonnull IndexOptions indexOptions) throws IOException {
        super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, false, true, highlight);
        this.state = state;
        this.blenderType = blenderType;
        this.indexOptions = indexOptions;
        this.numFactor = numFactor;
        if (exponent != null) {
            this.exponent = exponent;
        }
    }

    @Override
    public List<Lookup.LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) throws IOException {
        // Don't * numFactor here since we do it down below, once, in the call chain:
        return super.lookup(key, contexts, onlyMorePopular, num);
    }

    @Override
    public List<Lookup.LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
        // Don't * numFactor here since we do it down below, once, in the call chain:
        return super.lookup(key, contexts, num, allTermsRequired, doHighlight);
    }

    @Override
    public List<Lookup.LookupResult> lookup(CharSequence key, Map<BytesRef, BooleanClause.Occur> contextInfo, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
        // Don't * numFactor here since we do it down below, once, in the call chain:
        return super.lookup(key, contextInfo, num, allTermsRequired, doHighlight);
    }

    @Override
    public List<Lookup.LookupResult> lookup(CharSequence key, BooleanQuery contextQuery, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
        /** We need to do num * numFactor here only because it is the last call in the lookup chain*/
        return super.lookup(key, contextQuery, num * numFactor, allTermsRequired, doHighlight);
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

    @Override
    protected FieldType getTextFieldType() {
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(indexOptions);
        ft.setOmitNorms(true);

        return ft;
    }

    @Override
    protected List<LookupResult> createResults(IndexSearcher searcher, TopFieldDocs hits, int num, CharSequence key,
                                               boolean doHighlight, Set<String> matchedTokens, String prefixToken)
            throws IOException {

        NavigableSet<LookupResult> results = new TreeSet<>(LOOKUP_COMP);

        // we reduce the num to the one initially requested
        int actualNum = num / numFactor;

        for (int i = 0; i < hits.scoreDocs.length; i++) {
            FieldDoc fd = (FieldDoc) hits.scoreDocs[i];

            BinaryDocValues textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);
            assert textDV != null;

            textDV.advance(fd.doc);

            final String text = textDV.binaryValue().utf8ToString();
            long weight = (Long) fd.fields[0];

            // This will just be null if app didn't pass payloads to build():
            // TODO: maybe just stored fields?  they compress...
            BinaryDocValues payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");

            BytesRef payload;
            if (payloadsDV != null) {
                if (payloadsDV.advance(fd.doc) == fd.doc) {
                    payload = BytesRef.deepCopyOf(payloadsDV.binaryValue());
                } else {
                    payload = new BytesRef(BytesRef.EMPTY_BYTES);
                }
            } else {
                payload = null;
            }

            double coefficient;
            if (text.startsWith(key.toString())) {
                // if hit starts with the key, we don't change the score
                coefficient = 1;
            } else {
                coefficient = createCoefficient(matchedTokens, prefixToken, text, payload);
            }
            if (weight == 0) {
                weight = 1;
            }
            if (weight < 1 / LINEAR_COEF && weight > -1 / LINEAR_COEF) {
                weight *= 1 / LINEAR_COEF;
            }
            long score = (long) (weight * coefficient);

            LookupResult result;
            if (doHighlight) {
                result = new LookupResult(text, highlight(text, matchedTokens, prefixToken), score, payload);
            } else {
                result = new LookupResult(text, score, payload);
            }
            
            if (results.size() >= actualNum) {
                if (results.first().value < result.value) {
                    results.pollFirst();
                } else {
                    continue;
                }
            }
            results.add(result);
        }

        return new ArrayList<>(results.descendingSet());
    }

    private double createCoefficient(Set<String> matchedTokens, String prefixToken,
                                     String text, BytesRef payload) throws IOException {
        final Tuple fieldTuple = Tuple.fromBytes(payload.bytes);
        final String fieldName = (String) fieldTuple.get(0);
        Integer position = Integer.MAX_VALUE;
        try (TokenStream tokenStream = indexAnalyzer.tokenStream(fieldName, text)) {
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            int p = 0;
            while (tokenStream.incrementToken()) {
                String term = charTermAttribute.toString();
                if (matchedTokens.contains(term) || (prefixToken != null && term.startsWith(prefixToken))) {
                    position = p;
                    break;
                }
                p++;
            }
        }

        // create corresponding coefficient based on position
        return calculateCoefficient(position);
    }

    private double calculateCoefficient(int position) {
        double coefficient;
        switch (blenderType) {
            case POSITION_LINEAR:
                coefficient = 1 - LINEAR_COEF * position;
                break;
            case POSITION_RECIPROCAL:
                coefficient = 1. / (position + 1);
                break;
            case POSITION_EXPONENTIAL_RECIPROCAL:
                coefficient = 1. / Math.pow((position + 1.0), exponent);
                break;
            default:
                throw new RecordCoreArgumentException("Invalid blender type for Lucene auto-complete suggestion search: " + blenderType.name());
        }

        return coefficient;
    }

    @SuppressWarnings("serial")
    private static class LookUpComparator implements Comparator<Lookup.LookupResult>, Serializable {
        @Override
        public int compare(Lookup.LookupResult o1, Lookup.LookupResult o2) {
            // order on weight
            if (o1.value > o2.value) {
                return 1;
            } else if (o1.value < o2.value) {
                return -1;
            }

            // otherwise on alphabetic order
            int keyCompare = CHARSEQUENCE_COMPARATOR.compare(o1.key, o2.key);

            if (keyCompare != 0) {
                return keyCompare;
            }

            // if same weight and title, use the payload if there is one
            if (o1.payload != null) {
                return o1.payload.compareTo(o2.payload);
            }

            return 0;
        }
    }
}

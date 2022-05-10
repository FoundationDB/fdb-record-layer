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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneLoggerInfoStream;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Optimized {@link BlendedInfixSuggester} that does not rely on term vectors persisted in DB.
 * The implementation of methods {@link #getIndexWriterConfig(Analyzer, IndexWriterConfig.OpenMode)}, {@link #getTextFieldType()} and {@link #createCoefficient(Set, String, String, BytesRef)}
 * are the main differences between this and {@link BlendedInfixSuggester}.
 * This implementation also overrides the {@link AnalyzingInfixSuggester#lookup(CharSequence, BooleanQuery, int, boolean, boolean)} to also support phrase query.
 */
public class LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors extends AnalyzingInfixSuggester {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOptimizedBlendedInfixSuggesterWithoutTermVectors.class);
    private static final Comparator<LookupResult> LOOKUP_COMP = new LookUpComparator();
    /**
     * Coefficient used for linear blending.
     */
    private static final double LINEAR_COEF = 0.10;

    /**
     * How we sort the postings and search results.
     */
    private static final Sort SORT = new Sort(new SortField("weight", SortField.Type.LONG, true));

    @Nonnull
    private final IndexMaintainerState state;
    @Nonnull
    private final IndexOptions indexOptions;

    /**
     * Type of blender used by the suggester.
     */
    @Nonnull
    private final BlendedInfixSuggester.BlenderType blenderType;

    /**
     * Factor to multiply the number of searched elements.
     */
    private final int numFactor;

    /**
     * A copy of its superclass's minPrefixChars.
     */
    private final int minPrefixCharsCopy;

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
        this.minPrefixCharsCopy = minPrefixChars;
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

    /**
     * This one is different from the implementation by {@link BlendedInfixSuggester}.
     * This method supports phrase query, whereas the {@link BlendedInfixSuggester} does not.
     */
    @Override
    public List<Lookup.LookupResult> lookup(CharSequence key, BooleanQuery contextQuery, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
        if (searcherMgr == null) {
            throw new IllegalStateException("suggester was not built");
        }

        final BooleanClause.Occur occur;
        if (allTermsRequired) {
            occur = BooleanClause.Occur.MUST;
        } else {
            occur = BooleanClause.Occur.SHOULD;
        }

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        Set<String> matchedTokens = new HashSet<>();

        final boolean phraseQueryNeeded = key.toString().startsWith("\"") && key.toString().endsWith("\"");
        final String searchKey = phraseQueryNeeded ? key.toString().substring(1, key.toString().length() - 1) : key.toString();

        String prefixToken = phraseQueryNeeded
                             ? buildQueryForPhraseMatching(query, matchedTokens, searchKey, occur)
                             : buildQueryForTermsMatching(query, matchedTokens, searchKey, occur);

        if (contextQuery != null) {
            boolean allMustNot = true;
            for (BooleanClause clause : contextQuery.clauses()) {
                if (clause.getOccur() != BooleanClause.Occur.MUST_NOT) {
                    allMustNot = false;
                    break;
                }
            }

            if (allMustNot) {
                // All are MUST_NOT: add the contextQuery to the main query instead (not as sub-query)
                for (BooleanClause clause : contextQuery.clauses()) {
                    query.add(clause);
                }
            } else if (allTermsRequired == false) {
                // We must carefully upgrade the query clauses to MUST:
                BooleanQuery.Builder newQuery = new BooleanQuery.Builder();
                newQuery.add(query.build(), BooleanClause.Occur.MUST);
                newQuery.add(contextQuery, BooleanClause.Occur.MUST);
                query = newQuery;
            } else {
                // Add contextQuery as sub-query
                query.add(contextQuery, BooleanClause.Occur.MUST);
            }
        }

        // TODO: we could allow blended sort here, combining
        // weight w/ score.  Now we ignore score and sort only
        // by weight:

        Query finalQuery = finishQuery(query, allTermsRequired);

        //System.out.println("finalQuery=" + finalQuery);

        // Sort by weight, descending:
        TopFieldCollector c = TopFieldCollector.create(SORT, num * numFactor, 1);
        List<LookupResult> results = null;
        SearcherManager mgr;
        IndexSearcher searcher;
        synchronized (searcherMgrLock) {
            mgr = searcherMgr; // acquire & release on same SearcherManager, via local reference
            searcher = mgr.acquire();
        }
        try {
            //System.out.println("got searcher=" + searcher);
            searcher.search(finalQuery, c);

            TopFieldDocs hits = c.topDocs();

            // Slower way if postings are not pre-sorted by weight:
            // hits = searcher.search(query, null, num, SORT);
            results = createResults(searcher, hits, num * numFactor, key, doHighlight, matchedTokens, prefixToken);
        } finally {
            mgr.release(searcher);
        }

        //System.out.println((System.currentTimeMillis() - t0) + " msec for infix suggest");
        //System.out.println(results);

        return results;
    }

    /**
     * This one is different from the implementation by {@link BlendedInfixSuggester}.
     * This method overrides the {@link IndexWriterConfig}, whereas the {@link BlendedInfixSuggester} does not.
     */
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
                    String logMsg = KeyValueLogMessage.of("Auto-complete index mergeSource",
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

    /**
     * This one is different from the implementation by {@link BlendedInfixSuggester}.
     * This does not enable term vectors, and has the indexOptions configurable.
     */
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

    /**
     * This one is different from the implementation by {@link BlendedInfixSuggester}.
     * This one figures out the positions for matches by tokenizing the text and finding the matched tokens from it,
     * instead of relying on the term vectors.
     */
    private double createCoefficient(Set<String> matchedTokens, String prefixToken,
                                     String text, BytesRef payload) throws IOException {
        final String fieldName = payload == null ? "" : (String) Tuple.fromBytes(payload.bytes).get(0);
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

    @Nullable
    private String buildQueryForPhraseMatching(@Nonnull BooleanQuery.Builder query, @Nonnull Set<String> matchedTokens,
                                               @Nonnull String searchKey, @Nonnull BooleanClause.Occur occur) throws IOException {
        String prefixToken = null;
        try (TokenStream ts = queryAnalyzer.tokenStream("", new StringReader(searchKey))) {
            //long t0 = System.currentTimeMillis();
            ts.reset();
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            String lastToken = null;
            PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            int maxEndOffset = -1;
            while (ts.incrementToken()) {
                if (lastToken != null) {
                    matchedTokens.add(lastToken);
                    phraseQueryBuilder.add(new Term(TEXT_FIELD_NAME, lastToken));
                }
                lastToken = termAtt.toString();
                if (lastToken != null) {
                    maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
                }
            }
            ts.end();

            if (lastToken != null) {
                if (maxEndOffset == offsetAtt.endOffset()) {
                    // Use PrefixQuery (or the ngram equivalent) when
                    // there was no trailing discarded chars in the
                    // string (e.g. whitespace), so that if query does
                    // not end with a space we show prefix matches for
                    // that token:
                    query.add(getPhrasePrefixQuery(phraseQueryBuilder.build(), lastToken), occur);
                    prefixToken = lastToken;
                } else {
                    // Use TermQuery for an exact match if there were
                    // trailing discarded chars (e.g. whitespace), so
                    // that if query ends with a space we only show
                    // exact matches for that term:
                    matchedTokens.add(lastToken);
                    phraseQueryBuilder.add(new Term(TEXT_FIELD_NAME, lastToken));
                    query.add(phraseQueryBuilder.build(), occur);
                }
            }
        }
        return prefixToken;
    }

    @Nullable
    private String buildQueryForTermsMatching(@Nonnull BooleanQuery.Builder query, @Nonnull Set<String> matchedTokens,
                                              @Nonnull String searchKey, @Nonnull BooleanClause.Occur occur) throws IOException {
        String prefixToken = null;
        try (TokenStream ts = queryAnalyzer.tokenStream("", new StringReader(searchKey))) {
            //long t0 = System.currentTimeMillis();
            ts.reset();
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            String lastToken = null;
            int maxEndOffset = -1;
            while (ts.incrementToken()) {
                if (lastToken != null) {
                    matchedTokens.add(lastToken);
                    query.add(new TermQuery(new Term(TEXT_FIELD_NAME, lastToken)), occur);
                }
                lastToken = termAtt.toString();
                if (lastToken != null) {
                    maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
                }
            }
            ts.end();

            if (lastToken != null) {
                Query lastQuery;
                if (maxEndOffset == offsetAtt.endOffset()) {
                    // Use PrefixQuery (or the ngram equivalent) when
                    // there was no trailing discarded chars in the
                    // string (e.g. whitespace), so that if query does
                    // not end with a space we show prefix matches for
                    // that token:
                    lastQuery = getLastTokenQuery(lastToken);
                    prefixToken = lastToken;
                } else {
                    // Use TermQuery for an exact match if there were
                    // trailing discarded chars (e.g. whitespace), so
                    // that if query ends with a space we only show
                    // exact matches for that term:
                    matchedTokens.add(lastToken);
                    lastQuery = new TermQuery(new Term(TEXT_FIELD_NAME, lastToken));
                }

                if (lastQuery != null) {
                    query.add(lastQuery, occur);
                }
            }
        }
        return prefixToken;
    }

    private Query getPhrasePrefixQuery(@Nonnull PhraseQuery phraseQuery, @Nonnull String lastToken) {
        Term[] terms = phraseQuery.getTerms();
        SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(TEXT_FIELD_NAME, true); // field
        for (Term term : terms) {
            spanQuery.addClause(new SpanTermQuery(term));
        }

        SpanQuery lastTokenQuery = lastToken.length() < minPrefixCharsCopy
                                   ? new SpanTermQuery(new Term(TEXTGRAMS_FIELD_NAME, new BytesRef(lastToken.getBytes(StandardCharsets.UTF_8))))
                                   : new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(TEXT_FIELD_NAME, lastToken)));
        FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(lastTokenQuery, TEXT_FIELD_NAME);
        spanQuery.addClause(fieldMask);

        return spanQuery.build();
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

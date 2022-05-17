/*
 * LuceneAutoCompleteResultCursor.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class is a Record Cursor implementation for Lucene auto complete suggestion lookup.
 * Because use cases of auto complete never need to get a big number of suggestions in one call, no scan with continuation support is needed.
 * Suggestion result is populated as an {@link IndexEntry}, the key is in {@link IndexEntry#getKey()}, the field where it is indexed from and the value are in {@link IndexEntry#getValue()}.
 */
public class LuceneAutoCompleteResultCursor implements BaseCursor<IndexEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneAutoCompleteResultCursor.class);
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final IndexMaintainerState state;
    @Nonnull
    private final String query;
    @Nullable
    private final FDBStoreTimer timer;
    private int limit;
    @Nullable
    private RecordCursor<RecordCursorResult<IndexEntry>> lookupResults = null;
    private int currentPosition;
    @Nullable
    private final Tuple groupingKey;
    private final boolean highlight;
    private final Analyzer queryAnalyzer;
    private final List<String> fieldNames;

    public LuceneAutoCompleteResultCursor(@Nonnull String query,
                                          @Nonnull Executor executor, @Nonnull ScanProperties scanProperties,
                                          @Nonnull Analyzer queryAnalyzer, @Nonnull IndexMaintainerState state,
                                          @Nullable Tuple groupingKey, @Nonnull List<String> fieldNames, boolean highlight) {
        if (query.isEmpty()) {
            throw new RecordCoreArgumentException("Invalid query for auto-complete search")
                    .addLogInfo(LogMessageKeys.QUERY, query)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        }

        this.query = query;
        this.executor = executor;
        this.limit = Math.min(scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION));
        this.timer = state.context.getTimer();
        this.currentPosition = 0;
        if (scanProperties.getExecuteProperties().getSkip() > 0) {
            this.currentPosition += scanProperties.getExecuteProperties().getSkip();
        }
        this.highlight = highlight;
        this.state = state;
        this.groupingKey = groupingKey;
        this.queryAnalyzer = queryAnalyzer;
        this.fieldNames = fieldNames;
    }

    private synchronized IndexReader getIndexReader() throws IOException {
        return FDBDirectoryManager.getManager(state).getIndexReader(groupingKey);
    }

    @SuppressWarnings("cast")
    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        return CompletableFuture.supplyAsync(() -> {
            if (lookupResults == null) {
                try {
                    performLookup();
                } catch (IOException ioException) {
                    throw new RecordCoreException("Exception to lookup the auto complete suggestions", ioException)
                            .addLogInfo(LogMessageKeys.QUERY, query);
                }
            }
            RecordCursorResult<RecordCursorResult<IndexEntry>> next = lookupResults.getNext();
            if (next.hasNext()) {
                return next.get();
            }
            return RecordCursorResult.exhausted();
        }, executor);
    }

    @Nonnull
    private static RecordCursorContinuation continuationHelper(String key, long value, byte[] payload) {
        LuceneContinuationProto.LuceneAutoCompleteIndexContinuation.Builder continuationBuilder = LuceneContinuationProto.LuceneAutoCompleteIndexContinuation.newBuilder().setKey(key);
        continuationBuilder.setValue(value);
        continuationBuilder.setPayload(ByteString.copyFrom(payload));
        return ByteArrayContinuation.fromNullable(continuationBuilder.build().toByteArray());
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    private void performLookup() throws IOException {
        if (lookupResults != null) {
            return;
        }
        long startTime = System.nanoTime();

        IndexReader indexReader = getIndexReader();
        IndexSearcher searcher = new IndexSearcher(indexReader, executor);

        lookupResults = lookup(query, null, limit, true, highlight);
        if (timer != null) {
            timer.recordSinceNanoTime(LuceneEvents.Events.LUCENE_AUTO_COMPLETE_SUGGESTIONS_SCAN, startTime);
            timer.increment(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS, 6); // TODO JL
        }
    }

    /** Override this method to customize the Object
     *  representing a single highlighted suggestions; the
     *  result is set on each {@link
     *  org.apache.lucene.search.suggest.Lookup.LookupResult#highlightKey} member. */
    protected String highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
        try (TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            StringBuilder sb = new StringBuilder();
            int upto = 0;
            while (ts.incrementToken()) {
                String token = termAtt.toString();
                int startOffset = offsetAtt.startOffset();
                int endOffset = offsetAtt.endOffset();
                if (upto < startOffset) {
                    addNonMatch(sb, text.substring(upto, startOffset));
                    upto = startOffset;
                } else if (upto > startOffset) {
                    continue;
                }

                if (matchedTokens.contains(token)) {
                    // Token matches.
                    addWholeMatch(sb, text.substring(startOffset, endOffset), token);
                    upto = endOffset;
                } else if (prefixToken != null && token.startsWith(prefixToken)) {
                    addPrefixMatch(sb, text.substring(startOffset, endOffset), token, prefixToken);
                    upto = endOffset;
                }
            }
            ts.end();
            int endOffset = offsetAtt.endOffset();
            if (upto < endOffset) {
                addNonMatch(sb, text.substring(upto));
            }
            return sb.toString();
        }
    }

    /** Called while highlighting a single result, to append a
     *  non-matching chunk of text from the suggestion to the
     *  provided fragments list.
     *  @param sb The {@code StringBuilder} to append to
     *  @param text The text chunk to add
     */
    protected void addNonMatch(StringBuilder sb, String text) {
        sb.append(text);
    }

    /** Called while highlighting a single result, to append
     *  the whole matched token to the provided fragments list.
     *  @param sb The {@code StringBuilder} to append to
     *  @param surface The surface form (original) text
     *  @param analyzed The analyzed token corresponding to the surface form text
     */
    protected void addWholeMatch(StringBuilder sb, String surface, String analyzed) {
        sb.append("<b>");
        sb.append(surface);
        sb.append("</b>");
    }

    /** Called while highlighting a single result, to append a
     *  matched prefix token, to the provided fragments list.
     *  @param sb The {@code StringBuilder} to append to
     *  @param surface The fragment of the surface form
     *        (indexed during build, corresponding to
     *        this match
     *  @param analyzed The analyzed token that matched
     *  @param prefixToken The prefix of the token that matched
     */
    protected void addPrefixMatch(StringBuilder sb, String surface, String analyzed, String prefixToken) {
        // TODO: apps can try to invert their analysis logic
        // here, e.g. downcase the two before checking prefix:
        if (prefixToken.length() >= surface.length()) {
            addWholeMatch(sb, surface, analyzed);
            return;
        }
        sb.append("<b>");
        sb.append(surface.substring(0, prefixToken.length()));
        sb.append("</b>");
        sb.append(surface.substring(prefixToken.length()));
    }

    public RecordCursor<RecordCursorResult<IndexEntry>> lookup(CharSequence key, BooleanQuery contextQuery, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {

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
                             ? buildQueryForPhraseMatching(fieldNames, query, matchedTokens, searchKey, occur)
                             : buildQueryForTermsMatching(fieldNames, query, matchedTokens, searchKey, occur);

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


        Query finalQuery = finishQuery(query, allTermsRequired);

        try {
            IndexReader indexReader = getIndexReader();
            IndexSearcher searcher = new IndexSearcher(indexReader, executor);
            TopDocs topDocs = searcher.search(finalQuery, limit);
            return createResults(searcher, topDocs, limit, key, doHighlight, matchedTokens, prefixToken);
        } finally {
            // searcher not closed todo
        }
    }

    @Nullable
    private String buildQueryForPhraseMatching(@Nonnull List<String> fieldNames, @Nonnull BooleanQuery.Builder query, @Nonnull Set<String> matchedTokens,
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
                    final String probeToken = lastToken;
                    fieldNames.forEach(f -> phraseQueryBuilder.add(new Term(f, probeToken)));
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
                    final String probeToken = lastToken;
                    fieldNames.forEach(f -> query.add(getPhrasePrefixQuery(f, phraseQueryBuilder.build(), probeToken), occur));
                    prefixToken = lastToken;
                } else {
                    // Use TermQuery for an exact match if there were
                    // trailing discarded chars (e.g. whitespace), so
                    // that if query ends with a space we only show
                    // exact matches for that term:
                    matchedTokens.add(lastToken);
                    final String probeToken = lastToken;
                    fieldNames.forEach(f -> phraseQueryBuilder.add(new Term(f, probeToken)));
                    query.add(phraseQueryBuilder.build(), occur);
                }
            }
        }
        return prefixToken;
    }

    @Nullable
    private String buildQueryForTermsMatching(@Nonnull List<String> fieldNames, @Nonnull BooleanQuery.Builder query, @Nonnull Set<String> matchedTokens,
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
                    final String probeToken = lastToken;
                    fieldNames.forEach(f -> query.add(new TermQuery(new Term(f, probeToken)), occur));
                }
                lastToken = termAtt.toString();
                if (lastToken != null) {
                    maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
                }
            }
            ts.end();

            if (lastToken != null) {
                List<Query> lastQuery;
                if (maxEndOffset == offsetAtt.endOffset()) {
                    // Use PrefixQuery (or the ngram equivalent) when
                    // there was no trailing discarded chars in the
                    // string (e.g. whitespace), so that if query does
                    // not end with a space we show prefix matches for
                    // that token:
                    lastQuery = getLastTokenQuery(fieldNames, lastToken);
                    prefixToken = lastToken;
                } else {
                    // Use TermQuery for an exact match if there were
                    // trailing discarded chars (e.g. whitespace), so
                    // that if query ends with a space we only show
                    // exact matches for that term:
                    final String probeToken = lastToken;
                    matchedTokens.add(lastToken);
                    lastQuery = fieldNames.stream().map(f -> new TermQuery(new Term(f, probeToken))).collect(Collectors.toList());
                }

                if (lastQuery != null) {
                    lastQuery.forEach(q -> query.add(q, occur));
                }
            }
        }
        return prefixToken;
    }

    private Query getPhrasePrefixQuery(@Nonnull String fieldName, @Nonnull PhraseQuery phraseQuery, @Nonnull String lastToken) {
        Term[] terms = phraseQuery.getTerms();
        SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(fieldName, true); // field
        for (Term term : terms) {
            spanQuery.addClause(new SpanTermQuery(term));
        }
        SpanQuery lastTokenQuery = new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(fieldName, lastToken)));
        FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(lastTokenQuery, fieldName);
        spanQuery.addClause(fieldMask);

        return spanQuery.build();
    }

    /** Subclass can override this to tweak the Query before
     *  searching. */
    protected Query finishQuery(BooleanQuery.Builder in, boolean allTermsRequired) {
        return in.build();
    }

    /** This is called if the last token isn't ended
     *  (e.g. user did not type a space after it).  Return an
     *  appropriate Query clause to add to the BooleanQuery. */
    protected List<Query> getLastTokenQuery(List<String> fieldNames, final String token) throws IOException {
        return fieldNames.stream().map(f -> new PrefixQuery(new Term(f, token))).collect(Collectors.toList());
    }

    protected RecordCursor<RecordCursorResult<IndexEntry>> createResults(IndexSearcher searcher, TopDocs topDocs, int num,
                                                      CharSequence charSequence,
                                                      boolean doHighlight, Set<String> matchedTokens, String prefixToken)
            throws IOException {
        return RecordCursor.fromIterator(Arrays.stream(topDocs.scoreDocs).iterator())
                .mapPipelined(scoreDoc -> {
            try {
                IndexableField primaryKey = searcher.doc(scoreDoc.doc).getField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME);
                BytesRef pk = primaryKey.binaryValue();
                return state.store.loadRecordAsync(Tuple.fromBytes(pk.bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, state.store.getPipelineSize(PipelineOperation.KEY_TO_RECORD))
                .filter(Objects::nonNull)
                .map(state.store::queriedRecord)
                .map(result -> {

                    final KeyExpression rootExpression = state.index.getRootExpression();
                    final List<Key.Evaluated> indexKeys = rootExpression.evaluate(result);
                    String value = (String) indexKeys.get(0).values().get(0); // TODO
                    try {
                        if (highlight) {
                            value = highlight(value, matchedTokens, prefixToken);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Tuple key = result.getPrimaryKey().add("text").add(value); // TODO
                    if (groupingKey != null) {
                        key = groupingKey.addAll(key);
                    }
                    IndexEntry indexEntry = new IndexEntry(state.index, key, Tuple.from(100)); // TODO
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Suggestion read as an index entry={}", indexEntry);
                    }
                    return RecordCursorResult.withNextValue(indexEntry, continuationHelper("", 100, "sdfds".getBytes()));
                });
    }

}

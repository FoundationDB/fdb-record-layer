/*
 * LuceneSpellcheckResultCursor.java
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
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

/**
 * Cursor over Lucene spell-check query results.
 */
public class LuceneSpellCheckRecordCursor implements BaseCursor<IndexEntry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSpellCheckRecordCursor.class);
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final IndexMaintainerState state;
    private final int limit;
    @Nonnull
    private final String wordToSpellCheck;
    @Nonnull
    private final DirectSpellChecker spellchecker;
    @Nullable
    private final FDBStoreTimer timer;

    private IndexReader indexReader;

    @Nullable
    private List<IndexEntry> spellcheckSuggestions = null;
    private int currentPosition = 0;
    @Nullable
    private final Tuple groupingKey;
    @Nullable
    private final Integer partitionId;
    private final List<String> fields;
    private boolean closed;


    public LuceneSpellCheckRecordCursor(@Nonnull List<String> fields,
                                        @Nonnull String wordToSpellCheck,
                                        @Nonnull final Executor executor,
                                        final ScanProperties scanProperties,
                                        @Nonnull final IndexMaintainerState state,
                                        @Nullable Tuple groupingKey,
                                        @Nullable Integer partitionId) {
        this.fields = fields;
        this.wordToSpellCheck = wordToSpellCheck;
        this.executor = executor;
        this.state = state;
        this.limit = Math.min(
                scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_SPELLCHECK_SEARCH_UPPER_LIMIT));
        this.groupingKey = groupingKey;
        this.partitionId = partitionId;
        this.spellchecker = new DirectSpellChecker();
        this.timer = state.context.getTimer();
        this.closed = false;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        CompletableFuture<IndexEntry> spellcheckResult = CompletableFuture.supplyAsync( () -> {
            if (spellcheckSuggestions == null) {
                try {
                    spellcheck();
                } catch (IOException e) {
                    throw LuceneExceptions.toRecordCoreException("Spellcheck suggestions lookup failure", e);
                }
            }
            return currentPosition < spellcheckSuggestions.size() ? spellcheckSuggestions.get(currentPosition) : null;
        }, executor);
        return spellcheckResult.thenApply(r -> {
            if (r == null) {
                return RecordCursorResult.exhausted();
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Suggestion read as an index entry={}", spellcheckSuggestions.get(currentPosition));
                }
                return RecordCursorResult.withNextValue(r, continuationHelper(spellcheckSuggestions.get(currentPosition++)));
            }
        });
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper(@Nonnull IndexEntry lookupResult) {
        LuceneContinuationProto.LuceneSpellCheckIndexContinuation.Builder continuationBuilder =
                LuceneContinuationProto.LuceneSpellCheckIndexContinuation.newBuilder().setValue(ByteString.copyFromUtf8(lookupResult.toString()));
        continuationBuilder.setLocation(currentPosition);
        return ByteArrayContinuation.fromNullable(continuationBuilder.build().toByteArray());
    }

    @Override
    public void close() {
        if (indexReader != null) {
            IOUtils.closeWhileHandlingException(indexReader);
        }
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    private synchronized IndexReader getIndexReader() throws IOException {
        return FDBDirectoryManager.getManager(state).getIndexReaderWithReplayedQueue(groupingKey, partitionId);
    }

    private void spellcheck() throws IOException {
        if (spellcheckSuggestions != null) {
            return;
        }
        final long startTime = System.nanoTime();
        indexReader = getIndexReader();

        List<Suggestion> suggestionResults = new ArrayList<>();
        for (String field : fields) {
            //collect all suggestions across all given fields or across all fields depending on user options.
            Arrays.stream(spellchecker.suggestSimilar(new Term(field, wordToSpellCheck), limit, indexReader))
                    .map(suggestion -> new Suggestion(field, suggestion))
                    .forEach(suggestionResults::add);
        }
        spellcheckSuggestions = suggestionResults.stream()
                //Merge matching suggestions from different
                .collect(Collectors.toMap(
                        suggestion -> suggestion.suggestWord.string,
                        Function.identity(),
                        // TODO: For arnaud, are we checking for a merge on ALL suggested words against eachother?
                        LuceneSpellCheckRecordCursor::mergeTwoSuggestWords))
                .values()
                .stream()
                // Sort the suggested words from large to small by score then by frequency then by the field.
                .sorted(comparing((Suggestion s) -> s.suggestWord.score).reversed()
                                .thenComparing(comparing((Suggestion s) -> s.suggestWord.freq).reversed())
                                .thenComparing(s -> s.suggestWord.string))
                .limit(limit)
                // Map the words from suggestions to index entries.
                .map(suggestion -> {
                    Tuple key = Tuple.from(suggestion.indexField, suggestion.suggestWord.string);
                    if (groupingKey != null) {
                        key = groupingKey.addAll(key);
                    }
                    return new IndexEntry(state.index, key, Tuple.from(suggestion.suggestWord.score));
                })
                .collect(Collectors.toList());
        if (timer != null) {
            timer.recordSinceNanoTime(LuceneEvents.Events.LUCENE_SPELLCHECK_SCAN, startTime);
            timer.increment(LuceneEvents.Counts.LUCENE_SCAN_SPELLCHECKER_SUGGESTIONS, spellcheckSuggestions.size());
        }
    }

    private static class Suggestion {
        final String indexField;
        final SuggestWord suggestWord;

        public Suggestion(final String indexField, final SuggestWord suggestWord) {
            this.indexField = indexField;
            this.suggestWord = suggestWord;
        }
    }

    private static Suggestion mergeTwoSuggestWords(Suggestion a, Suggestion b) {
        int freq = a.suggestWord.freq + b.suggestWord.freq;
        String field;
        if (a.suggestWord.freq == b.suggestWord.freq) {
            // select the field based on string comparison
            field = a.indexField.compareTo(b.indexField) < 0 ? a.indexField : b.indexField;
        } else {
            // select the field with the highest frequency of the suggested word.
            field = a.suggestWord.freq > b.suggestWord.freq ? a.indexField : b.indexField;
        }
        SuggestWord newSuggestWord = new SuggestWord();
        newSuggestWord.freq = freq;
        newSuggestWord.score = a.suggestWord.score;
        newSuggestWord.string = a.suggestWord.string;
        return new Suggestion(field, newSuggestWord);
    }
}

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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LogMessageKeys;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.DirectoryCommitCheckAsync.getOrCreateDirectoryCommitCheckAsync;
import static com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync.getIndexWriterCommitCheckAsync;

public class LuceneSpellcheckRecordCursor implements BaseCursor<IndexEntry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSpellcheckRecordCursor.class);
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
    private List<IndexEntry> spellcheckSuggestions = null;
    private int currentPosition = 0;
    @Nullable
    private Tuple groupingKey;
    private final String[] fields;


    public LuceneSpellcheckRecordCursor(@Nonnull final String value,
                                        @Nonnull final Executor executor,
                                        final ScanProperties scanProperties,
                                        @Nonnull final IndexMaintainerState state,
                                        @Nullable Tuple groupingKey, final String[] fieldNames) {
        if (value.contains(":")) {
            String[] fieldAndWord = value.split(":", 2);
            if (Arrays.stream(fieldNames).noneMatch(name -> name.equals(fieldAndWord[0]))) {
                throw new RecordCoreException("Invalid field name in Lucene index query")
                        .addLogInfo(LogMessageKeys.FIELD_NAME, fieldAndWord[0])
                        .addLogInfo(LogMessageKeys.INDEX_FIELDS, fieldNames);
            }
            fields = new String[] {fieldAndWord[0]};
            wordToSpellCheck = fieldAndWord[1];
        } else {
            fields = fieldNames;
            wordToSpellCheck = value;
        }
        this.executor = executor;
        this.state = state;
        this.limit = Math.min(
                scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_SPELLCHECK_SEARCH_UPPER_LIMIT));
        this.groupingKey = groupingKey;
        this.spellchecker = new DirectSpellChecker();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        CompletableFuture<IndexEntry> spellcheckResult = CompletableFuture.supplyAsync( () -> {
            if (spellcheckSuggestions == null) {
                try {
                    spellcheck();
                } catch (IOException e) {
                    throw new RecordCoreException("Spellcheck suggestions lookup failure", e);
                }
            }
            return currentPosition < spellcheckSuggestions.size() ? spellcheckSuggestions.get(currentPosition) : null;
                }, executor);
        return spellcheckResult.thenApply(r -> {
            if (r == null) {
                return RecordCursorResult.exhausted();
            } else {
                return RecordCursorResult.withNextValue(r, continuationHelper(spellcheckSuggestions.get(currentPosition++)));
            }
        });
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper(@Nonnull IndexEntry lookupResult) {
        RecordCursorProto.LuceneSpellcheckIndexContinuation.Builder continuationBuilder =
                RecordCursorProto.LuceneSpellcheckIndexContinuation.newBuilder().setValue(ByteString.copyFromUtf8(lookupResult.toString()));
        continuationBuilder.setLocation(currentPosition);
        return ByteArrayContinuation.fromNullable(continuationBuilder.build().toByteArray());
    }

    @Override
    public void close() {

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
        IndexWriterCommitCheckAsync writerCheck = getIndexWriterCommitCheckAsync(state, groupingKey);
        return writerCheck == null ? DirectoryReader.open(getOrCreateDirectoryCommitCheckAsync(state, groupingKey).getDirectory()) : DirectoryReader.open(writerCheck.indexWriter);
    }

    /**
     * Reduces the SuggestWords returned over the fields to a single SuggestWord with the field it appears the most in.
     * In the case of a tie the first field it appeared in takes precedence.
     * @param results - never empty due to the fact that the suggested word needs to appear somewhere for it to be
     *     suggested at all.
     * @return a single pair of String which is the field it most occurs in and SuggestWord that contains the total
     *     frequency over the queried fields, the score of the match, the suggested string
     */
    private Pair<String, SuggestWord> reduceSuggestedResults(List<Pair<String, SuggestWord>> results) {
        SuggestWord finalSuggestion = new SuggestWord();
        int highestFreq = 0;
        String mostFreqField = results.get(0).getLeft();
        finalSuggestion.string = results.get(0).getRight().string;
        finalSuggestion.score = results.get(0).getRight().score;
        finalSuggestion.freq = 0;
        for (Pair<String, SuggestWord> suggestion : results) {
            finalSuggestion.freq += suggestion.getRight().freq;
            if (highestFreq < suggestion.getRight().freq) mostFreqField = suggestion.getLeft();
        }
        return ImmutablePair.of(mostFreqField, finalSuggestion);
    }

    /**
     * A comparison function which reverses the sorting order. We want the list sorted high -> low.
     * @param s1
     * @param s2
     * @return the comparison between the pairs of suggested words.
     */
    private int compareSuggestWords(Pair<String, SuggestWord> s1, Pair<String, SuggestWord> s2) {
        int compareResult = Float.compare(s2.getRight().score, s1.getRight().score);
        if (compareResult != 1) return compareResult;
        compareResult = Integer.compare(s2.getRight().freq, s1.getRight().freq);
        if (compareResult != 1) return compareResult;
        return s1.getLeft().compareTo(s2.getLeft());
    }

    private void spellcheck() throws IOException {
        if (spellcheckSuggestions != null) {
            return;
        }
        Multimap<String, Pair<String, SuggestWord>> suggestionResultsMap = HashMultimap.create();
        long startTime = System.nanoTime();
        IndexReader indexReader = getIndexReader();
        for (String field : fields) {
            Arrays.stream(spellchecker.suggestSimilar(new Term(field, wordToSpellCheck), limit, indexReader))
                    .map(suggestion -> ImmutablePair.of(field, suggestion))
                    .forEach(fieldAndSuggestion -> suggestionResultsMap.put(fieldAndSuggestion.getRight().string, fieldAndSuggestion));
        }
        List<Pair<String, SuggestWord>> suggestionResultsList = new ArrayList<>();
        for (String suggestionKey : suggestionResultsMap.keys()) {
            suggestionResultsList.add(reduceSuggestedResults(List.copyOf(suggestionResultsMap.get(suggestionKey))));
        }
        suggestionResultsList.sort(this::compareSuggestWords);
//        Collections.reverse(suggestionResultsList);
        spellcheckSuggestions = suggestionResultsList.stream()
                .limit(limit)
                .map(suggestion -> new IndexEntry(state.index, Tuple.from(suggestion.getRight().string),
                        Tuple.from(suggestion.getLeft())))
                .collect(Collectors.toList());
        // hello text 5x
        // hello text 2x    hello text1 3x    score 0.5
        // help text 2x     help text1 3x     score 0.5
        //TODO add metric via timer.
    }
}

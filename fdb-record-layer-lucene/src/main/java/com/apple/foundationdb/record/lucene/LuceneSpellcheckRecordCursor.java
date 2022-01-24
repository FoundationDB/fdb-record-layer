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
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
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
    private IndexReader indexReader;
    private String[] fields;


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
        this.fields = fieldNames;
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

    private void spellcheck() throws IOException {
        if (spellcheckSuggestions != null) {
            return;
        }
        spellcheckSuggestions = new ArrayList<>();
        long startTime = System.nanoTime();
        indexReader = getIndexReader();
        for (String field : fields) {
            Arrays.stream(spellchecker.suggestSimilar(new Term(field, wordToSpellCheck), limit, indexReader))
                    .map(suggestion -> new IndexEntry(state.index, Tuple.from(suggestion.string), Tuple.from(field)))
                    .forEach(spellcheckSuggestions::add);
        }
        //TODO add metric via timer.
    }
}

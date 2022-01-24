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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
    private DirectSpellChecker spellchecker;

    @Nullable
    private SuggestWord[] spellcheckSuggestions = null;
    private int currentPosition = 0;
    @Nullable
    private Tuple groupingKey;
    private IndexReader indexReader;


    public LuceneSpellcheckRecordCursor(@Nonnull final String value,
                                        @Nonnull final Executor executor,
                                        final ScanProperties scanProperties,
                                        @Nonnull final IndexMaintainerState state,
                                        @Nullable Tuple groupingKey) {
        this.wordToSpellCheck = value;
        this.executor = executor;
        this.state = state;
        this.limit = Math.min(
                scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_SPELLCHECK_SEARCH_UPPER_LIMIT));
        this.groupingKey = groupingKey;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        CompletableFuture<String> spellcheckResult = CompletableFuture.supplyAsync( () -> {
            if (spellcheckSuggestions == null) {
                try {
                    spellcheck();
                } catch (IOException e) {
                    throw new RecordCoreException("Spellcheck suggestions lookup failure", e);
                }
            }
            return currentPosition < spellcheckSuggestions.length ? spellcheckSuggestions[currentPosition].string : null;
                }, executor);
        return spellcheckResult.thenApply(r -> {
            if (r == null) {
                return RecordCursorResult.exhausted();
            } else {
                IndexEntry entry = new IndexEntry(state.index, Tuple.from(r), Tuple.from(r));
                return RecordCursorResult.withNextValue(entry, continuationHelper(spellcheckSuggestions[currentPosition++]));
            }
        });
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper(@Nonnull SuggestWord lookupResult) {
        RecordCursorProto.LuceneSpellcheckIndexContinuation.Builder continuationBuilder =
                RecordCursorProto.LuceneSpellcheckIndexContinuation.newBuilder().setValue(ByteString.copyFromUtf8(lookupResult.string));
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
        if (spellchecker == null) {
            spellchecker = new DirectSpellChecker();
        }
        long startTime = System.nanoTime();
        indexReader = getIndexReader();
        spellcheckSuggestions = spellchecker.suggestSimilar(new Term("text", wordToSpellCheck), limit, indexReader);
        //TODO add metric via timer.
    }
}

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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.search.suggest.Lookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class LuceneSpellcheckResultCursor implements BaseCursor<IndexEntry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSpellcheckResultCursor.class);
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final IndexMaintainerState state;
    private final int limit;
    @Nonnull
    private final String wordToSpellCheck;
    @Nonnull
    private final SpellChecker spellchecker;

    @Nullable
    private List<Lookup.LookupResult> spellcheckSuggestions = null;
    private int currentPosition = 0;

    public LuceneSpellcheckResultCursor(@Nonnull final SpellChecker spellchecker,
                                        @Nonnull final String value,
                                        @Nonnull final Executor executor,
                                        final ScanProperties scanProperties,
                                        @Nonnull final IndexMaintainerState state) {
        this.spellchecker = spellchecker;
        this.wordToSpellCheck = value;
        this.executor = executor;
        this.state = state;
        this.limit = Math.min(
                scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_SPELLCHECK_SEARCH_UPPER_LIMIT));
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        CompletableFuture<Lookup.LookupResult> spellcheckResult = CompletableFuture.supplyAsync( () -> {
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
                if (r.payload == null) {
                    throw new RecordCoreException("Empty payload of lookup result for lucene spellcheck suggestions");
                }
                IndexEntry indexEntry = new IndexEntry(state.index, Tuple.from(r.key), Tuple.from(r.payload.utf8ToString(), r.value));
                return RecordCursorResult.withNextValue(indexEntry, continuationHelper(spellcheckSuggestions.get(currentPosition++)));
            }
        });
    }

    @Nonnull
    private static RecordCursorContinuation continuationHelper(@Nonnull Lookup.LookupResult lookupResult) {
        RecordCursorProto.LuceneSpellcheckIndexContinuation.Builder continuationBuilder =
                RecordCursorProto.LuceneSpellcheckIndexContinuation.newBuilder().setKey((String) lookupResult.key);
        continuationBuilder.setValue(lookupResult.value);
        continuationBuilder.setPayload(ByteString.copyFrom(lookupResult.payload.bytes));
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

    private void spellcheck() throws IOException {
        if (spellcheckSuggestions != null) {
            return;
        }
        long startTime = System.nanoTime();
        //spellcheckSuggestions = spellchecker.suggestSimilar(wordToSpellCheck, limit);

    }
}

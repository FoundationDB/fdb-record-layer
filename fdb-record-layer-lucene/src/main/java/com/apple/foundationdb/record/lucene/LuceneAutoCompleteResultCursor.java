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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
    private List<Lookup.LookupResult> lookupResults = null;
    private int currentPosition;
    @Nullable
    private final Tuple groupingKey;
    @Nonnull
    private final AnalyzingInfixSuggester suggester;
    private final boolean highlight;

    public LuceneAutoCompleteResultCursor(@Nonnull AnalyzingInfixSuggester suggester, @Nonnull String query,
                                          @Nonnull Executor executor, @Nonnull ScanProperties scanProperties,
                                          @Nonnull IndexMaintainerState state, @Nullable Tuple groupingKey, boolean highlight) {
        if (query.isEmpty()) {
            throw new RecordCoreArgumentException("Invalid query for auto-complete search")
                    .addLogInfo(LogMessageKeys.QUERY, query)
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        }

        this.suggester = suggester;
        this.query = query;
        this.executor = executor;
        this.state = state;
        this.limit = Math.min(scanProperties.getExecuteProperties().getReturnedRowLimitOrMax(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION));
        this.timer = state.context.getTimer();
        this.currentPosition = 0;
        if (scanProperties.getExecuteProperties().getSkip() > 0) {
            this.currentPosition += scanProperties.getExecuteProperties().getSkip();
        }
        this.groupingKey = groupingKey;
        this.highlight = highlight;
    }

    @SuppressWarnings("cast")
    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        CompletableFuture<Lookup.LookupResult> lookupResult = CompletableFuture.supplyAsync(() -> {
            if (lookupResults == null) {
                try {
                    performLookup();
                } catch (IOException ioException) {
                    throw new RecordCoreException("Exception to lookup the auto complete suggestions", ioException)
                            .addLogInfo(LogMessageKeys.QUERY, query);
                }
            }
            return currentPosition < lookupResults.size() ? lookupResults.get(currentPosition) : null;
        }, executor);

        return lookupResult.thenApply(r -> {
            if (r == null) {
                return RecordCursorResult.exhausted();
            } else {
                final String suggestion = highlight ? (String) r.highlightKey : (String) r.key;

                if (r.payload == null) {
                    throw new RecordCoreException("Empty payload of lookup result for lucene auto complete suggestion")
                            .addLogInfo(LogMessageKeys.QUERY, query)
                            .addLogInfo(LogMessageKeys.RESULT, suggestion);
                }

                Tuple key = Tuple.fromBytes(r.payload.bytes).add(suggestion);
                if (groupingKey != null) {
                    key = groupingKey.addAll(key);
                }
                IndexEntry indexEntry = new IndexEntry(state.index, key, Tuple.from(r.value));

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Suggestion read as an index entry={}", indexEntry);
                }

                return RecordCursorResult.withNextValue(indexEntry, continuationHelper(lookupResults.get(currentPosition++)));
            }
        });
    }

    @Nonnull
    private static RecordCursorContinuation continuationHelper(@Nonnull Lookup.LookupResult lookupResult) {
        LuceneContinuationProto.LuceneAutoCompleteIndexContinuation.Builder continuationBuilder = LuceneContinuationProto.LuceneAutoCompleteIndexContinuation.newBuilder().setKey((String) lookupResult.key);
        continuationBuilder.setValue(lookupResult.value);
        continuationBuilder.setPayload(ByteString.copyFrom(lookupResult.payload.bytes));
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
        lookupResults = suggester.getCount() > 0
                        ? suggester.lookup(query, Collections.emptySet(), limit, true, highlight)
                        : Collections.emptyList();
        if (timer != null) {
            timer.recordSinceNanoTime(LuceneEvents.Events.LUCENE_AUTO_COMPLETE_SUGGESTIONS_SCAN, startTime);
            timer.increment(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS, lookupResults.size());
        }
    }
}

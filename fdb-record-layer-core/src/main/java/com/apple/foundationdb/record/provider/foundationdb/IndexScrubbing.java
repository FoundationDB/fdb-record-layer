/*
 * IndexScrubbing.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *  Scrub a readable index to validate its consistency. Repair if allowed and needed.
 *  This general scrubber will use the index maintainers to get specific index scrubbing functionality.
 */
@API(API.Status.INTERNAL)
public class IndexScrubbing extends IndexingBase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexScrubbing.class);
    @Nonnull
    private static final IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = compileIndexingTypeStamp();
    @Nonnull
    private final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy;
    @Nonnull
    private final AtomicLong issueCounter;
    private long scanCounter = 0;
    private int logWarningCounter;
    private final IndexScrubbingTools.ScrubbingType scrubbingType;
    private final String scrubberName;

    public IndexScrubbing(@Nonnull final IndexingCommon common,
                          @Nonnull final OnlineIndexer.IndexingPolicy policy,
                          @Nonnull final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy,
                          @Nonnull final AtomicLong issueCounter,
                          IndexScrubbingTools.ScrubbingType scrubbingType) {
        super(common, policy, true);
        this.scrubbingPolicy = scrubbingPolicy;
        this.logWarningCounter = scrubbingPolicy.getLogWarningsLimit();
        this.issueCounter = issueCounter;
        this.scrubbingType = scrubbingType;
        scrubberName = "scrub " + scrubbingType + " entries for " + common.getIndex().getType() + " index";
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, scrubberName,
                LogMessageKeys.ALLOW_REPAIR, scrubbingPolicy.allowRepair(),
                LogMessageKeys.RANGE_ID, scrubbingPolicy.getScrubbingRangeId(),
                LogMessageKeys.RANGE_RESET, scrubbingPolicy.isScrubbingRangeReset(),
                LogMessageKeys.SCRUB_TYPE, scrubbingType,
                LogMessageKeys.SCAN_LIMIT, scrubbingPolicy.getEntriesScanLimit()
        );
    }

    @Nonnull
    @Override
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(final FDBRecordStore store) {
        return myIndexingTypeStamp;
    }

    @Nonnull
    static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp() {
        return
                IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR)
                        .build();
    }

    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return getRunner().runAsync(context ->
                        context.getReadVersionAsync().thenCompose(ignore -> indexScrub()),
                common.indexLogMessageKeyValues("IndexScrubbing::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> indexScrub() {

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "indexScrub");

        return iterateAllRanges(additionalLogMessageKeyValues,
                this::indexScrubRangeOnly);
    }

    @Nonnull
    private CompletableFuture<Boolean> indexScrubRangeOnly(@Nonnull FDBRecordStore store, @Nonnull AtomicLong recordsScanned) {
        Index index = common.getIndex();
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if (recordMetaDataProvider == null || !metaData.equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        final IndexScrubbingTools<?> tools = maintainer.getIndexScrubbingTools(scrubbingType);
        if (tools == null) {
            throw new UnsupportedOperationException("This index does not support scrubbing type " + scrubbingType);
        }

        return indexScrubRangeOnly(store, recordsScanned, index, tools, maintainer.isIdempotent());
    }

    private <T> CompletableFuture<Boolean> indexScrubRangeOnly(final @Nonnull FDBRecordStore store, final @Nonnull AtomicLong recordsScanned, final Index index, final IndexScrubbingTools<T> tools, boolean isIdempotent) {
        // scrubbing only scannable index (in readable or readable-unique-pending state)
        validateOrThrowEx(store.getIndexState(index).isScannable(), "scrubbed index is not readable");
        // scrubbing only idempotent indexes (at least for now)
        validateOrThrowEx(isIdempotent, "scrubbed index is not idempotent");

        final IndexingRangeSet rangeSet = getRangeset(store, index);
        tools.presetCommonParams(index, scrubbingPolicy.allowRepair(), common.getIndexContext().isSynthetic, common.getAllRecordTypes());

        return rangeSet.firstMissingRangeAsync().thenCompose(range -> {
            if (range == null) {
                // Here: no more missing ranges - all done
                // This scrubbing is done. Clear the rangeSet - the next time scrubbing is called it will start from scratch
                logScrubberRangeReset("range exhausted");
                rangeSet.clear();
                return AsyncUtil.READY_FALSE;
            }
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            final RecordCursor<T> cursor = tools.getCursor(tupleRange, store, getLimit() + 1); // always respect limit in this path; +1 allows a continuation item in forward scan
            final AtomicBoolean hasMore = new AtomicBoolean(true);
            final AtomicReference<RecordCursorResult<T>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final long scanLimit = scrubbingPolicy.getEntriesScanLimit();
            List<IndexScrubbingTools.Issue> issueList = new LinkedList<>();

            return iterateRangeOnly(store, cursor, (recordStore, result) -> handleOneItem(recordStore, result, tools, issueList),
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenApply(vignore -> hasMore.get() ? tools.getKeyFromCursorResult(lastResult.get()) : rangeEnd)
                    .thenCompose(continuation -> updateRangeAndCheckIfExhausted(rangeSet, rangeStart, rangeEnd, continuation))
                    .thenApply(ret -> checkScanLimit(ret, recordsScanned, scanLimit))
                    .whenComplete((ignore, err) -> reportIssues(issueList, err));
        });
    }

    private <T> CompletableFuture<FDBStoredRecord<Message>> handleOneItem(FDBRecordStore store, final RecordCursorResult<T> result, final IndexScrubbingTools<T> tools, List<IndexScrubbingTools.Issue> issueList) {
        return tools.handleOneItem(store, result)
                .thenApply(issue -> {
                    if (issue == null) {
                        return null;
                    }
                    issueList.add(issue);
                    return issue.recordToIndex;
                });
    }

    private static CompletableFuture<Boolean> updateRangeAndCheckIfExhausted(final IndexingRangeSet rangeSet, final Tuple rangeStart, final Tuple rangeEnd, final Tuple continuation) {
        return rangeSet.insertRangeAsync(packOrNull(rangeStart), packOrNull(continuation), true)
                .thenApply(ignore -> notAllRangesExhausted(continuation, rangeEnd));
    }

    private Boolean checkScanLimit(final Boolean ret, final @Nonnull AtomicLong recordsScanned, final long scanLimit) {
        if (scanLimit > 0) {
            scanCounter += recordsScanned.get();
            if (scanLimit <= scanCounter) {
                return false;
            }
        }
        return ret;
    }

    private void reportIssues(List<IndexScrubbingTools.Issue> issueList, Throwable err) {
        if (err != null || issueList == null || issueList.isEmpty()) {
            // either no issue to report (the common case), or avoid reporting after an exception
            return;
        }
        // report these issues only after their transaction was completed successfully
        for (IndexScrubbingTools.Issue issue: issueList) {
            issueCounter.incrementAndGet();
            if (issue.logMessage != null && LOGGER.isWarnEnabled() && logWarningCounter > 0) {
                logWarningCounter --;
                LOGGER.warn(issue.logMessage
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
            if (issue.timerCounter != null) {
                timerIncrement(issue.timerCounter);
            }
        }
    }

    IndexingRangeSet getRangeset(FDBRecordStore store, Index index) {
        switch (scrubbingType) {
            case MISSING:
                return IndexingRangeSet.forScrubbingRecords(store, index, scrubbingPolicy.getScrubbingRangeId());
            case DANGLING:
                return IndexingRangeSet.forScrubbingIndex(store, index, scrubbingPolicy.getScrubbingRangeId());
            default:
                throw new RecordCoreArgumentException("Unpredicted scrubbing type ");
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    @Override
    protected CompletableFuture<Void> setScrubberTypeOrThrow(FDBRecordStore store) {
        // HERE: The index must be readable, checked by the caller.
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);
        validateOrThrowEx(indexingTypeStamp.getMethod().equals(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR),
                "Not a scrubber type-stamp");

        final Index index = common.getIndex(); // Note: multi targets mode is not supported (yet)
        final IndexingRangeSet rangeSet = getRangeset(store, index);
        if (scrubbingPolicy.isScrubbingRangeReset()) {
            logScrubberRangeReset("forced reset");
            rangeSet.clear();
            return AsyncUtil.DONE;
        }
        return rangeSet.firstMissingRangeAsync()
                .thenAccept(recordRange -> {
                    if (recordRange == null) {
                        // Here: no un-scrubbed range is available for this call. Erase the 'ranges' data to allow
                        // a new, fresh records re-scrubbing.
                        logScrubberRangeReset("range exhausted detected");
                        rangeSet.clear();
                    }
                });
    }

    private void logScrubberRangeReset(String reason) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .addKeyAndValue(LogMessageKeys.REASON, reason)
                    .toString());
        }
    }

    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(final FDBRecordStore store) {
        throw new UnsupportedOperationException();
    }
}

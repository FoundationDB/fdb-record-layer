/*
 * IndexingMutuallyByRecords.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This indexer supports mutual concurrent multi-target indexing by multiple processes or hosts. To do it, each indexer
 * must be called with the exact same arguments.
 * The problem that we try to solve:
 *    Sometimes, building indexes for a store with many records takes too long. To speed up indexing, we wish to perform
 *    the task in parallel by multiple entities - threads, processes, or hosts.
 * The problem that we do not try to solve:
 *    This solution is not optimized for small stores, or for short-lived processes. We can safely assume that the indexing,
 *    in each process, takes long enough to consider Order(1) metadata-operations as neglectable.
 * For flexibility, we wish to be able to build index(es) by multiple entities. And also:
 *   - Be consistent - each process can bring the indexing process to success.
 *   - Be efficient - all the processes should participate in the build until success.
 *   - Let the user use any arbitrary number of processes, yet keep said efficiency and consistency.
 *   - Assume no locality - the only interaction between the processes is the fdb database itself.
 *   - Be able to add indexing processes on the fly.
 *   - Survive halted/crashed processes while keeping consistency, and with minimal harm to the efficiency.
 *   - Avoid exposing the implementation details to the caller.
 *   - Allow a seamless continuation if the indexes were partly built in this mode.
 * The solution:
 *   Init:
 *   - Divide the records space to fragments. Assume that other processes get the same boundaries, but do not break if they do not.
 *   - Get a random step and a starting point - the step is a prime number that is not a divisor of the fragments count. This
 *     is designed to provide each process a unique iteration order over the fragments, but will not break if it is not unique.
 *   Build:
 *   - One iteration in FULL mode - only build fragments that are fully unbuilt (for efficiency).
 *   - One iteration in ANY mode - build any fragment that has a missing range.
 *   - Stop when the indexes are readable, convert to readable if there are no more missing ranges to build.
 */
public class IndexingMutuallyByRecords extends IndexingBase {
    private IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = null;
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingMutuallyByRecords.class);

    private List<Tuple> fragmentBoundaries;
    private int fragmentNum;
    private int fragmentStep;
    private int fragmentFirst;
    private int fragmentCurrent;
    private FragmentIterationType fragmentIterationType;
    private int loopProtectionCounter = 0;
    private String loopProtectionToken = "";

    private FDBException anyJumperEx = null;
    private int anyJumperCurrent;
    private Range anyJumperRange;

    enum FragmentIterationType {
        FULL,     // 1st iteration: only build fragments that are fully unbuilt (for efficiency).
        ANY,      // 2nd iteration: build any fragment that has a missing range.
        RECOVER   // 3rd iteration: presently throws an error, letting the caller handle recovery..
    }

    public IndexingMutuallyByRecords(@Nonnull final IndexingCommon common, @Nonnull final OnlineIndexer.IndexingPolicy policy,
                                     @Nullable List<Tuple> fragmentBoundaries) {
        super(common, policy);
        this.fragmentBoundaries = fragmentBoundaries;
        validateOrThrowEx(!policy.isReverseScanOrder(), "Mutual indexing does not support reverse scan order");
    }

    @Override
    @Nonnull
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store) {
        if (myIndexingTypeStamp == null) {
            myIndexingTypeStamp = compileIndexingTypeStamp(common.getTargetIndexesNames());
        }
        return myIndexingTypeStamp;
    }

    @Nonnull
    private static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp(List<String> targetIndexes) {
        if (targetIndexes.isEmpty()) {
            throw new ValidationException("No target index was set");
        }
        return IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS)
                .addAllTargetIndex(targetIndexes)
                .build();
    }

    private static boolean areTheyAllIdempotent(@Nonnull FDBRecordStore store, List<Index> targetIndexes) {
        return targetIndexes.stream()
                .allMatch(targetIndex -> store.getIndexMaintainer(targetIndex).isIdempotent());
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        List<Object> list = new ArrayList<>();
        list.addAll(Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "mutual multi target by records",
                LogMessageKeys.TARGET_INDEX_NAME, common.getTargetIndexesNames()));
        list.addAll(fragmentLogMessageKeyValues());
        return list;
    }

    private List<Tuple> getPrimaryKeyBoundaries(@Nonnull FDBRecordStore store) {
        TupleRange tupleRange = common.computeRecordsRange();
        store.getContext().getReadVersion(); // for instrumentation reasons
        List<Tuple> boundaries;
        try (RecordCursor<Tuple> cursor = store.getPrimaryKeyBoundaries(tupleRange)) {
            boundaries = cursor.asList().join();
        }

        if (boundaries == null) {
            boundaries = new ArrayList<>();
        }

        // Add the two endpoints to the range
        if (tupleRange == null) {
            // Here: make sure that the boundaries cover everything
            boundaries.add(0, null);
            boundaries.add(null);
        } else {
            // Here: add the endpoints, unless they are already included
            if (boundaries.isEmpty() || tupleRange.getLow() == null || tupleRange.getLow().compareTo(boundaries.get(0)) < 0) {
                boundaries.add(0, tupleRange.getLow());
            }
            if (tupleRange.getHigh() == null || tupleRange.getHigh().compareTo(boundaries.get(boundaries.size() - 1)) > 0) {
                boundaries.add(tupleRange.getHigh());
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("got boundaries",
                    LogMessageKeys.INDEX_NAME, common.getTargetIndexesNames(),
                    LogMessageKeys.RANGE, tupleRange,
                    LogMessageKeys.KEY_COUNT, boundaries.size()));
        }

        return boundaries;
    }

    private int getPrimeStep(int size, Random rn) {
        // Find a prime step that isn't size's factor
        validateOrThrowEx(size > 0, "No ranges to build");
        if (size < 3) {
            return 1;
        }
        for (int attempts = 0; attempts < 800; attempts ++) {
            int step = rn.nextInt(size);
            if ((step & 1) == 0) {
                step --;
            }
            if (step < 2) {
                return 1;
            }
            // Note: The real protection is in "size % step". The isPrime is used to reduce the chances of two indexers
            // using steps with common divisors - which may cause a correlated iteration order.
            if ((size % step != 0) && IntMath.isPrime(step)) {
                return step;
            }
        }
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(KeyValueLogMessage.of("too many attempts to generate random prime. Using step 1",
                    LogMessageKeys.TARGET_INDEX_NAME, common.getTargetIndexesNames()));
        }
        return 1;
    }

    private void setFragmentationData(@Nonnull FDBRecordStore store) {
        if (fragmentBoundaries == null || fragmentBoundaries.isEmpty()) {
            fragmentBoundaries = getPrimaryKeyBoundaries(store);
        }
        final Random rn = ThreadLocalRandom.current();
        fragmentNum = fragmentBoundaries.size() - 1; // ranges between the boundaries = one less
        fragmentStep = getPrimeStep(fragmentNum, rn);
        fragmentFirst = rn.nextInt(fragmentNum);
        fragmentCurrent = fragmentFirst;
        fragmentIterationType = FragmentIterationType.FULL;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.build("fragmentation init values")
                            .addKeysAndValues(fragmentLogMessageKeyValues()).toString());
        }
    }

    private List<Object> fragmentLogMessageKeyValues() {
        return new ArrayList<>(Arrays.asList(
                LogMessageKeys.INDEXING_FRAGMENTATION_COUNT, fragmentNum,
                LogMessageKeys.INDEXING_FRAGMENTATION_STEP, fragmentStep,
                LogMessageKeys.INDEXING_FRAGMENTATION_FIRST, fragmentFirst,
                LogMessageKeys.INDEXING_FRAGMENTATION_CURRENT, fragmentCurrent,
                LogMessageKeys.INDEXING_FRAGMENTATION_TYPE, fragmentIterationType));
    }

    private Range fragmentGet() {
        int i = getFragmentCurrent();
        // Note that fragmentNum is fragmentBoundaries.size()-1
        Tuple low = fragmentBoundaries.get(i);
        Tuple high = fragmentBoundaries.get(i + 1);
        byte[] lowBytes = low == null ? new byte[]{(byte)0} : low.pack();
        byte[] highBytes = high == null ? new byte[]{(byte)0xff} : high.pack();
        return new Range(lowBytes, highBytes);
    }

    private int getFragmentCurrent() {
        return fragmentCurrent;
    }

    private void fragmentPlusPlus() {
        fragmentCurrent += fragmentStep;
        fragmentCurrent %= fragmentNum;
        if (getFragmentCurrent() == fragmentFirst) {
            fragmentIterationTypePlusPlus();
        }
    }

    private void fragmentIterationTypePlusPlus() {
        if (fragmentIterationType == FragmentIterationType.ANY) {
            fragmentIterationType = FragmentIterationType.RECOVER;
        }
        if (fragmentIterationType == FragmentIterationType.FULL) {
            fragmentIterationType = FragmentIterationType.ANY;
        }
    }

    @Nonnull
    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose( store -> context.getReadVersionAsync()
                        .thenCompose(vignore -> {
                            // init fragment data
                            setFragmentationData(store);
                            SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                            // validation checks, if any, will be performed here
                            return subspaceProvider.getSubspaceAsync(context)
                                    .thenCompose(subspace -> buildMultiTargetIndex(subspaceProvider, subspace));
                        })
                ),
                common.indexLogMessageKeyValues("IndexingMutuallyByRecords::buildIndexInternalAsync",
                        fragmentLogMessageKeyValues()));
    }

    @Nonnull
    private CompletableFuture<Void> buildMultiTargetIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {
        final TupleRange tupleRange = common.computeRecordsRange();
        final byte[] rangeStart;
        final byte[] rangeEnd;
        if (tupleRange == null) {
            rangeStart = rangeEnd = null;
        } else {
            final Range range = tupleRange.toRange();
            rangeStart = range.begin;
            rangeEnd = range.end;
        }

        final CompletableFuture<FDBRecordStore> maybePresetRangeFuture =
                rangeStart == null ?
                CompletableFuture.completedFuture(null) :
                buildCommitRetryAsync((store, recordsScanned) -> {
                    // Here: only records inside the defined records-range are relevant to the index. Hence, the completing range
                    // can be preemptively marked as indexed.
                    final List<Index> targetIndexes = common.getTargetIndexes();
                    final List<IndexingRangeSet> targetRangeSets = targetIndexes.stream()
                            .map(targetIndex -> IndexingRangeSet.forIndexBuild(store, targetIndex))
                            .collect(Collectors.toList());
                    return CompletableFuture.allOf(
                                    insertRanges(targetRangeSets, null, rangeStart),
                                    insertRanges(targetRangeSets, rangeEnd, null))
                            .thenApply(ignore -> null);
                }, null);

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "mutualMultiTargetIndex-wrapper",
                LogMessageKeys.RANGE_START, rangeStart,
                LogMessageKeys.RANGE_END, rangeEnd);

        return maybePresetRangeFuture.thenCompose(ignore ->
                iterateAllRanges(additionalLogMessageKeyValues,
                        (store, recordsScanned) -> buildRangeOnly(store, subspaceProvider, subspace),
                        subspaceProvider, subspace));
    }

    @Nonnull
    private CompletableFuture<Boolean> buildRangeOnly(@Nonnull FDBRecordStore store,
                                                      @Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {
        // return false when done
        /* Mutual indexing:
         * 1. detects missing ranges
         * 2. Iterator 1  finds a fragment to build
         * 2. Iterator 2 iterates this fragment and builds it
         */
        if (fragmentIterationType == FragmentIterationType.RECOVER) {
            // At this point, recovery should be done manually after investigation. In the future we may fallback
            // to a regular multi target indexing - with or without a type stamp change.
            // Note that if there are less unbuilt fragments than active indexers, we will expect the surplus indexer to
            // reach this iteration (see anyJumper below).
            throw new ValidationException("Mutual indexing failure - third iteration");
        }
        validateSameMetadataOrThrow(store);
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, common.getPrimaryIndex());
        return rangeSet.listMissingRangesAsync().thenCompose(missingRanges ->
                buildNextRangeOnly(sortAndSquash(missingRanges), subspaceProvider, subspace));
    }

    private CompletableFuture<Boolean> buildNextRangeOnly(List<Range> missingRanges,
                                                          @Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("buildNextRangeOnly",
                    LogMessageKeys.MISSING_RANGES, missingRanges));
        }
        if (missingRanges.isEmpty()) {
            return AsyncUtil.READY_FALSE; // all done, Hurray!
        }
        // Here:
        // for each fragmented step, if it should be built - build it and return true
        while (true) {
            final Range fragmentRange = fragmentGet();
            if (fragmentIterationType == FragmentIterationType.RECOVER) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("Entering recovery mode",
                            LogMessageKeys.SPLIT_RANGES, missingRanges));
                }
                return AsyncUtil.READY_TRUE; // let the caller handle the recovery
            }
            final boolean isFull = fragmentIterationType == FragmentIterationType.FULL;
            Range rangeToBuild = isFull ?
                                 fullyUnBuiltRange(missingRanges, fragmentRange) :
                                 partlyUnBuiltRange(missingRanges, fragmentRange);

            if (anyJumperSaysBuild(rangeToBuild)) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.build("fragment/range to build",
                                    LogMessageKeys.SCAN_TYPE, fragmentIterationType,
                                    LogMessageKeys.RANGE, rangeToBuild,
                                    LogMessageKeys.ORIGINAL_RANGE, fragmentRange)
                            .addKeysAndValues(fragmentLogMessageKeyValues())
                            .toString());
                }
                timerIncrement(isFull ?
                               FDBStoreTimer.Counts.MUTUAL_INDEXER_FULL_START :
                               FDBStoreTimer.Counts.MUTUAL_INDEXER_ANY_START);
                infiniteLoopProtection(rangeToBuild, missingRanges);
                final List<Object> additionalLogMessageKeyValues = new ArrayList<>(
                        Arrays.asList(LogMessageKeys.CALLING_METHOD, "mutualMultiTargetIndex",
                                LogMessageKeys.RANGE, rangeToBuild,
                                LogMessageKeys.ORIGINAL_RANGE, fragmentRange));
                additionalLogMessageKeyValues.addAll(fragmentLogMessageKeyValues());
                return iterateAllRanges(additionalLogMessageKeyValues,
                        (store, recordsScanned) -> buildThisRangeOnly(store, recordsScanned, rangeToBuild),
                        subspaceProvider, subspace,
                        anyJumperCallback(rangeToBuild)
                ).thenCompose(ignore -> AsyncUtil.READY_TRUE);
            }
            if (anyJumperEx == null) {
                timerIncrement(isFull ?
                               FDBStoreTimer.Counts.MUTUAL_INDEXER_FULL_DONE :
                               FDBStoreTimer.Counts.MUTUAL_INDEXER_ANY_DONE);
                fragmentPlusPlus();
            }
        }
    }

    private CompletableFuture<Boolean> buildThisRangeOnly(@Nonnull FDBRecordStore store, @Nonnull AtomicLong recordsScanned,
                                                          Range thisRange) {

        final List<Index> targetIndexes = common.getTargetIndexes();
        final List<IndexingRangeSet> targetRangeSets = targetIndexes.stream()
                .map(targetIndex -> IndexingRangeSet.forIndexBuild(store, targetIndex))
                .collect(Collectors.toList());

        final boolean isIdempotent = areTheyAllIdempotent(store, targetIndexes);
        final ScanProperties scanProperties = scanPropertiesWithLimits(isIdempotent);

        return targetRangeSets.get(0).firstMissingRangeAsync(thisRange.begin, thisRange.end).thenCompose(range -> {
            if (range == null) {
                return AsyncUtil.READY_FALSE; // no more missing ranges - all done
            }
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            RecordCursor<FDBStoredRecord<Message>> cursor =
                    store.scanRecords(tupleRange, null, scanProperties);

            final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final AtomicBoolean hasMore = new AtomicBoolean(true);

            return iterateRangeOnly(store, cursor,
                    this::getRecordIfTypeMatch,
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getPrimaryKey() :
                                          rangeEnd)
                    .thenCompose(cont -> insertRanges(targetRangeSets, packOrNull(rangeStart), packOrNull(cont))
                            .thenApply(ignore -> notAllRangesExhausted(cont, rangeEnd)));
        });
    }

    @VisibleForTesting
    @Nullable
    static Range fullyUnBuiltRange(List<Range> missingRanges, Range fragmentRange) {
        // Return the fragmentRange 'as is' if it appears to be fully un-built in the
        // missing ranges list. Else return null.
        // * Assuming that the missing ranges are sorted and squashed
        // * Optimize for a heavily fragmented missing ranges. (Should we perform a binary search?)
        for (Range range: missingRanges) {
            if (ByteArrayUtil.compareUnsigned(range.end, fragmentRange.end) >= 0) {
                // missing range is beyond fragment
                if (ByteArrayUtil.compareUnsigned(range.begin, fragmentRange.begin) <= 0) {
                    return notEmptyRange(fragmentRange);
                }
                break;
            }
        }
        return null;
    }

    @VisibleForTesting
    @Nullable
    static Range partlyUnBuiltRange(List<Range> missingRanges, Range fragmentRange) {
        // Return null if the range appears to be fully built in the missing ranges list. Else
        // return the un-built range of (fragmentRange ∩ missingRanges)
        // * Assuming that the missing ranges are sorted and squashed
        // * Less optimized than the fullyUnBuiltRanges (this function is expected to be called only a handful
        //   of times at the end of the indexing process)
        for (Range range: missingRanges) {
            if (ByteArrayUtil.compareUnsigned(range.begin, fragmentRange.end) > 0 ||
                    ByteArrayUtil.compareUnsigned(range.end, fragmentRange.begin) < 0) {
                // no range overlap
                continue;
            }
            // Here: some range overlap, shrink to fragmentRange ∩ range
            byte[] begin = ByteArrayUtil.compareUnsigned(range.begin, fragmentRange.begin) >= 0 ?
                           range.begin : fragmentRange.begin;
            byte[] end = ByteArrayUtil.compareUnsigned(range.end, fragmentRange.end) <= 0 ?
                         range.end : fragmentRange.end;
            if (ByteArrayUtil.compareUnsigned(begin, end) < 0) {
                // if non-empty range
                return notEmptyRange(new Range(begin, end));
            }
        }
        return null;
    }

    @Nullable
    private static Range notEmptyRange(Range range) {
        // check the (rare, maybe test only) case of empty ranges
        return ByteArrayUtil.compareUnsigned(range.begin, range.end) >= 0 ?
               null : range;
    }

    @VisibleForTesting
    static List<Range> sortAndSquash(List<Range> ranges) {
        ranges.sort((a, b) -> ByteArrayUtil.compareUnsigned(a.begin, b.begin));
        boolean squasshed = false;
        for (int i = 0; i < ranges.size() - 1; i++) {
            if (ByteArrayUtil.compareUnsigned(ranges.get(i).end, ranges.get(i + 1).begin) >= 0) {
                squasshed = true;
                ranges.set(i + 1, new Range(ranges.get(i).begin,
                        ByteArrayUtil.compareUnsigned(ranges.get(i).end, ranges.get(i + 1).end) >= 0 ?
                        ranges.get(i).end :
                        ranges.get(i + 1).end));
                ranges.set(i, null);
            }
        }
        return squasshed ?
               ranges.stream().filter(Objects::nonNull).collect(Collectors.toList()) :
               ranges;
    }

    private static CompletableFuture<Void> insertRanges(List<IndexingRangeSet> rangeSets,
                                                        byte[] start, byte[] end) {
        return AsyncUtil.whenAll(rangeSets.stream().map(set -> set.insertRangeAsync(start, end, true)).collect(Collectors.toList()));
    }

    private void infiniteLoopProtection(final Range range, final List<Range> missingRanges) {
        String token = range.toString();
        if (token.equals(loopProtectionToken)) {
            loopProtectionCounter ++;
            if (loopProtectionCounter > 1000) {
                throw new ValidationException("Potential infinite loop",
                        LogMessageKeys.RANGE, token,
                        LogMessageKeys.MISSING_RANGES, missingRanges);
            }
        } else {
            loopProtectionCounter = 0;
            loopProtectionToken = token;
        }
    }

    /**
     * anyJumper algo:
     *  During ANY or FULL iterations, we wish to avoid cases of two indexers competing on the same fragment.
     *  It is done by recording the exception and the fragment info, then - during the next iteration:
     *      If at the same fragment, same range: re-throw
     *      If at the same fragment, different range: do not compete, jump to the next fragment
     */
    boolean anyJumperSaysBuild(@Nullable Range rangeToBuild) {
        // Note: if this function doesn't throw, it must clear anyJumperEx
        if (rangeToBuild == null) {
            // Here: nothing to build in this fragment, the old exception (if present) is obsolete
            anyJumperEx = null;
            return false;
        }
        if (anyJumperEx == null) {
            // Here: no old exception, build this range
            return true;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.build("anyJumper: check if should jump",
                            "anyJumperRange", anyJumperRange,
                            "anyJumperCurrent", anyJumperCurrent,
                            "anyJumperEx", anyJumperEx,
                            LogMessageKeys.RANGE, rangeToBuild)
                    .addKeysAndValues(fragmentLogMessageKeyValues())
                    .toString());
        }
        if (anyJumperCurrent != fragmentCurrent) {
            // Here: new fragment, the old exception is obsolete
            anyJumperEx = null;
            return true;
        }
        if (anyJumperRange.equals(rangeToBuild)) {
            // Here: in hindsight, this exception was not caused by a rangeSet conflict. Rethrow it.
            throw anyJumperEx;
        }
        // Here: Another indexer is processing this fragment, clear the exception and jump to the next one.
        timerIncrement(FDBStoreTimer.Counts.MUTUAL_INDEXER_ANY_JUMP);
        anyJumperEx = null;
        return false;
    }

    private Function<FDBException, Optional<Boolean>> anyJumperCallback(final Range rangeToBuild) {
        return ex -> {
            if (ex == null || anyJumperEx != null) {
                // Here: Either not an fdb error or anyJumperSaysBuild is re-throwing. Do not interrupt.
                anyJumperEx = null;
                return Optional.empty();
            }
            // Here: this exception may or may not be a rangeSet conflict.
            // Memorize this info and handle it during the next iteration.
            anyJumperEx = ex;
            anyJumperRange = rangeToBuild;
            anyJumperCurrent = fragmentCurrent;
            return Optional.of(false);
        };
    }

    @SuppressWarnings("unused")
    private  CompletableFuture<FDBStoredRecord<Message>> getRecordIfTypeMatch(FDBRecordStore store, @Nonnull RecordCursorResult<FDBStoredRecord<Message>> cursorResult) {
        // No need to "translate" rec, so store is unused
        FDBStoredRecord<Message> rec = cursorResult.get();
        return recordIfInIndexedTypes(rec);
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store) {
        throw new ValidationException("Mutual inline rebuild doesn't make any sense");
    }
}

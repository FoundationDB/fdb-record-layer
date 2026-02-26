/*
 * SplitHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.FDBRecordStoreProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Helper classes for splitting records across multiple key-value pairs.
 */
@API(API.Status.INTERNAL)
public class SplitHelper {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitHelper.class);

    /**
     * If a record is greater than this size (in bytes),
     * it will be split into multiple kv pairs.
     */
    public static final int SPLIT_RECORD_SIZE = 100_000; // 100K

    /**
     * Special split point added to end of the key when a record is associated with a version.
     */
    public static final long RECORD_VERSION = -1L;

    /**
     * Value added to the end of the key when a record is not split.
     */
    public static final long UNSPLIT_RECORD = 0L;

    /**
     * Minimum index used when a record is split. The index is appended to the key, one for each split portion.
     */
    public static final long START_SPLIT_RECORD = 1L;

    private SplitHelper() {
    }

    /**
     * Save serialized representation using multiple keys if necessary.
     * @param context write transaction
     * @param subspace subspace to save in
     * @param key key within subspace
     * @param serialized serialized representation
     * @param version the version to store inline with this record
     */
    public static void saveWithSplit(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace,
                                     @Nonnull final Tuple key, @Nonnull final byte[] serialized, @Nullable final FDBRecordVersion version) {
        saveWithSplit(context, subspace, key, serialized, version, true, false, false, null, null);
    }

    /**
     * Save serialized representation using multiple keys if necessary, clearing only as much as needed.
     * @param context write transaction
     * @param subspace subspace to save in
     * @param key key within subspace
     * @param serialized serialized representation
     * @param version the version to store inline with this record
     * @param splitLongRecords <code>true</code> if multiple keys should be used; if <code>false</code>, <code>serialized</code> must fit in a single key
     * @param omitUnsplitSuffix if <code>splitLongRecords</code> is <code>false</code>, then this will omit a suffix added to the end of the key if <code>true</code> for backwards-compatibility reasons
     * @param clearBasedOnPreviousSizeInfo if <code>splitLongRecords</code>, whether to use <code>previousSizeInfo</code> to determine how much to clear
     * @param previousSizeInfo if <code>clearBasedOnPreviousSizeInfo</code>, the {@link FDBStoredSizes} for any old record, or <code>null</code> if there was no old record
     * @param sizeInfo optional size information to populate
     */
    @SuppressWarnings("PMD.CloseResource")
    public static void saveWithSplit(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace,
                                     @Nonnull final Tuple key, @Nonnull final byte[] serialized, @Nullable final FDBRecordVersion version,
                                     final boolean splitLongRecords, final boolean omitUnsplitSuffix,
                                     final boolean clearBasedOnPreviousSizeInfo, @Nullable final FDBStoredSizes previousSizeInfo,
                                     @Nullable SizeInfo sizeInfo) {
        if (omitUnsplitSuffix && version != null) {
            throw new RecordCoreArgumentException("Cannot include version in-line using old unsplit record format")
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                    .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()))
                    .addLogInfo(LogMessageKeys.VERSION, version);
        }
        final Transaction tr = context.ensureActive();
        if (serialized.length > SplitHelper.SPLIT_RECORD_SIZE) {
            if (!splitLongRecords) {
                throw new RecordCoreException("Record is too long to be stored in a single value; consider split_long_records")
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()))
                        .addLogInfo(LogMessageKeys.VALUE_SIZE, serialized.length);
            }
            writeSplitRecord(context, subspace, key, serialized, clearBasedOnPreviousSizeInfo, previousSizeInfo, sizeInfo);
        } else {
            if (splitLongRecords || previousSizeInfo == null || previousSizeInfo.isVersionedInline()) {
                clearPreviousSplitRecord(context, subspace, key, clearBasedOnPreviousSizeInfo, previousSizeInfo);
            }
            final Tuple recordKey;
            if (splitLongRecords || !omitUnsplitSuffix) {
                recordKey = key.add(SplitHelper.UNSPLIT_RECORD);
            } else {
                recordKey = key;
            }
            final byte[] keyBytes = subspace.pack(recordKey);
            tr.set(keyBytes, serialized);
            if (sizeInfo != null) {
                sizeInfo.set(keyBytes, serialized);
                sizeInfo.setSplit(false);
            }
        }
        writeVersion(context, subspace, key, version, sizeInfo);
    }

    @SuppressWarnings("PMD.CloseResource")
    private static void writeSplitRecord(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace,
                                         @Nonnull final Tuple key, @Nonnull final byte[] serialized,
                                         final boolean clearBasedOnPreviousSizeInfo, @Nullable final FDBStoredSizes previousSizeInfo,
                                         @Nullable SizeInfo sizeInfo) {
        final Transaction tr = context.ensureActive();
        final Subspace keySplitSubspace = subspace.subspace(key);
        clearPreviousSplitRecord(context, subspace, key, clearBasedOnPreviousSizeInfo, previousSizeInfo);
        long index = SplitHelper.START_SPLIT_RECORD;
        int offset = 0;
        while (offset < serialized.length) {
            int nextOffset = offset + SplitHelper.SPLIT_RECORD_SIZE;
            if (nextOffset > serialized.length) {
                nextOffset = serialized.length;
            }
            final byte[] keyBytes = keySplitSubspace.pack(index);
            final byte[] valueBytes = Arrays.copyOfRange(serialized, offset, nextOffset);
            tr.set(keyBytes, valueBytes);
            if (sizeInfo != null) {
                if (offset == 0) {
                    sizeInfo.set(keyBytes, valueBytes);
                    sizeInfo.setSplit(true);
                } else {
                    sizeInfo.add(keyBytes, valueBytes);
                }
            }
            index++;
            offset = nextOffset;
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private static void writeVersion(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace, @Nonnull final Tuple key,
                                     @Nullable final FDBRecordVersion version, @Nullable final SizeInfo sizeInfo) {
        if (version == null) {
            if (sizeInfo != null) {
                sizeInfo.setVersionedInline(false);
            }
            return;
        }
        final Transaction tr = context.ensureActive();
        final byte[] keyBytes = subspace.pack(key.add(RECORD_VERSION));
        final byte[] valueBytes = packVersion(version);
        if (version.isComplete()) {
            tr.set(keyBytes, valueBytes);
        } else {
            context.addVersionMutation(MutationType.SET_VERSIONSTAMPED_VALUE, keyBytes, valueBytes);
            context.addToLocalVersionCache(keyBytes, version.getLocalVersion());
        }
        if (sizeInfo != null) {
            sizeInfo.setVersionedInline(true);
            sizeInfo.add(keyBytes, valueBytes);
            if (!version.isComplete()) {
                // If the version isn't complete, an offset gets added to the
                // end of the value to indicate where the version should be
                // written. This is not made durable, so remove it from the metric.
                sizeInfo.valueSize -= Integer.BYTES;
            }
        }
    }

    public static void dryRunSaveWithSplitOnlySetSizeInfo(@Nonnull final Subspace subspace,
                                                          @Nonnull final Tuple key, @Nonnull final byte[] serialized, @Nullable final FDBRecordVersion version,
                                                          final boolean splitLongRecords, final boolean omitUnsplitSuffix,
                                                          @Nullable SizeInfo sizeInfo) {
        if (serialized.length > SplitHelper.SPLIT_RECORD_SIZE) {
            dryRunWriteSplitRecordOnlySetSizeInfo(subspace, key, serialized, sizeInfo);
        } else {
            final Tuple recordKey;
            if (splitLongRecords || !omitUnsplitSuffix) {
                recordKey = key.add(SplitHelper.UNSPLIT_RECORD);
            } else {
                recordKey = key;
            }
            final byte[] keyBytes = subspace.pack(recordKey);
            if (sizeInfo != null) {
                sizeInfo.set(keyBytes, serialized);
                sizeInfo.setSplit(false);
            }
        }
        dryRunWriteVersionSizeInfo(subspace, key, version, sizeInfo);
    }

    private static void dryRunWriteSplitRecordOnlySetSizeInfo(@Nonnull final Subspace subspace,
                                                              @Nonnull final Tuple key, @Nonnull final byte[] serialized,
                                                              @Nullable SizeInfo sizeInfo) {
        final Subspace keySplitSubspace = subspace.subspace(key);
        long index = SplitHelper.START_SPLIT_RECORD;
        int offset = 0;
        while (offset < serialized.length) {
            int nextOffset = offset + SplitHelper.SPLIT_RECORD_SIZE;
            if (nextOffset > serialized.length) {
                nextOffset = serialized.length;
            }
            final byte[] keyBytes = keySplitSubspace.pack(index);
            final byte[] valueBytes = Arrays.copyOfRange(serialized, offset, nextOffset);
            if (sizeInfo != null) {
                if (offset == 0) {
                    sizeInfo.set(keyBytes, valueBytes);
                    sizeInfo.setSplit(true);
                } else {
                    sizeInfo.add(keyBytes, valueBytes);
                }
            }
            index++;
            offset = nextOffset;
        }
    }

    private static void dryRunWriteVersionSizeInfo(@Nonnull final Subspace subspace, @Nonnull final Tuple key,
                                                   @Nullable final FDBRecordVersion version, @Nullable final SizeInfo sizeInfo) {
        if (version == null) {
            if (sizeInfo != null) {
                sizeInfo.setVersionedInline(false);
            }
            return;
        }
        final byte[] keyBytes = subspace.pack(key.add(RECORD_VERSION));
        final byte[] valueBytes = packVersion(version);
        if (sizeInfo != null) {
            sizeInfo.setVersionedInline(true);
            sizeInfo.add(keyBytes, valueBytes);
            if (!version.isComplete()) {
                // If the version isn't complete, an offset gets added to the
                // end of the value to indicate where the version should be
                // written. This is not made durable, so remove it from the metric.
                sizeInfo.valueSize -= Integer.BYTES;
            }
        }
    }


    @Nonnull
    static byte[] packVersion(@Nonnull FDBRecordVersion version) {
        if (version.isComplete()) {
            return Tuple.from(version.toVersionstamp(false)).pack();
        } else {
            return Tuple.from(version.toVersionstamp(false)).packWithVersionstamp();
        }
    }

    @Nullable
    static FDBRecordVersion unpackVersion(@Nullable byte[] packedVersion) {
        if (packedVersion != null) {
            return FDBRecordVersion.fromVersionstamp(Tuple.fromBytes(packedVersion).getVersionstamp(0), true);
        } else {
            return null;
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private static void clearPreviousSplitRecord(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace,
                                                 @Nonnull final Tuple key,
                                                 final boolean clearBasedOnPreviousSizeInfo, @Nullable FDBStoredSizes previousSizeInfo) {
        final Transaction tr = context.ensureActive();
        final Subspace keySplitSubspace = subspace.subspace(key);
        if (clearBasedOnPreviousSizeInfo) {
            if (previousSizeInfo != null) {
                if (Boolean.TRUE.equals(context.getPropertyStorage().getPropertyValue(FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES))) {
                    // Issue individual deletes for each key to allow underlying storage to optimize single key range
                    // clears. However, still include a write conflict range for the whole record to avoid adding extra
                    // work to the resolver.
                    Range keySplitSubspaceRange = keySplitSubspace.range();
                    tr.addWriteConflictRange(keySplitSubspaceRange.begin, keySplitSubspaceRange.end);
                    List<Long> offsets = offsets(previousSizeInfo);
                    for (Long offset : offsets) {
                        tr.clear(keySplitSubspace.pack(offset));
                    }
                } else {
                    if (previousSizeInfo.isSplit() || previousSizeInfo.isVersionedInline()) {
                        tr.clear(keySplitSubspace.range()); // Record might be shorter than previous split.
                    } else {
                        // Record was previously unsplit and had unsplit suffix because we are splitting long records.
                        tr.clear(keySplitSubspace.pack(UNSPLIT_RECORD));
                    }
                }
            }
        } else {
            tr.clear(keySplitSubspace.range()); // Clears both unsplit and previous longer split.
        }
        final byte[] versionKey = keySplitSubspace.pack(RECORD_VERSION);
        context.getLocalVersion(versionKey).ifPresent(localVersion -> context.removeVersionMutation(versionKey));
    }

    /**
     * Load serialized byte array that may be split among several keys.
     * @param tr read transaction
     * @param context transaction context
     * @param subspace subspace containing serialized value
     * @param key key within subspace
     * @param splitLongRecords <code>true</code> if multiple keys should be used; if <code>false</code>, <code>serialized</code> must fit in a single key
     * @param missingUnsplitRecordSuffix if <code>splitLongRecords</code> is <code>false</code> and this is <code>true</code>, this will assume keys are missing a suffix for backwards compatibility reasons
     * @param sizeInfo optional size information to populate
     * @return the merged byte array
     */
    public static CompletableFuture<FDBRawRecord> loadWithSplit(@Nonnull final ReadTransaction tr, @Nonnull final FDBRecordContext context,
                                                                @Nonnull final Subspace subspace, @Nonnull final Tuple key,
                                                                final boolean splitLongRecords, final boolean missingUnsplitRecordSuffix,
                                                                @Nullable SizeInfo sizeInfo) {
        if (!splitLongRecords && missingUnsplitRecordSuffix) {
            return loadUnsplitLegacy(tr, context, subspace, key, sizeInfo);
        }
        if (Boolean.TRUE.equals(context.getPropertyStorage().getPropertyValue(FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS))) {
            return loadSplitViaGets(tr, context, subspace, key, sizeInfo);
        }

        // Even if long records are not split, then unless we are using the old format, it might be the case
        // that there is a record version associated with this key, hence the range read.
        // It is still better to do a range read in that case than two point reads (probably).
        final long startTime = System.nanoTime();
        final Subspace recordSubspace = subspace.subspace(key);
        final Range recordRange = recordSubspace.range();
        // Note that recordSubspace.range() includes only keys that are a strict prefix of recordSubspace.pack(). This means
        // it excludes recordSubspace.pack() itself as well as any keys that are greater than or equal to recordSubspace.pack() + \xff.
        final AsyncIterable<KeyValue> rangeScan = scanSingleRecord(tr, recordRange.begin, recordRange.end);
        final AsyncIterator<KeyValue> rangeIter = rangeScan.iterator();
        context.instrument(FDBStoreTimer.DetailEvents.GET_RECORD_RANGE_RAW_FIRST_CHUNK, rangeIter.onHasNext(), startTime);
        return new SingleKeyUnsplitter(context, key, recordSubspace, rangeIter, sizeInfo).run(context.getExecutor());
    }

    private static CompletableFuture<FDBRawRecord> loadSplitViaGets(@Nonnull final ReadTransaction tr,
                                                                    @Nonnull final FDBRecordContext context,
                                                                    @Nonnull final Subspace subspace,
                                                                    @Nonnull final Tuple key,
                                                                    @Nullable SizeInfo sizeInfo) {
        final SizeInfo storedSizes = sizeInfo == null ? new SizeInfo() : sizeInfo;
        storedSizes.reset();

        final Subspace recordSubspace = subspace.subspace(key);
        final byte[] versionKey = recordSubspace.pack(RECORD_VERSION);
        final byte[] unsplitKey = recordSubspace.pack(UNSPLIT_RECORD);
        final byte[] startSplitKey = recordSubspace.pack(START_SPLIT_RECORD);

        // Cover the whole record range in a single read conflict range to decrease size of final commit request
        // (when compared to having individual read conflicts keys added for each key read)
        final Range recordRange = recordSubspace.range();
        tr.addReadConflictRangeIfNotSnapshot(recordRange.begin, recordRange.end);

        final CompletableFuture<FDBRecordVersion> versionValueFuture = context.getLocalVersion(versionKey)
                .map(FDBRecordVersion::incomplete)
                .map(CompletableFuture::completedFuture)
                .orElseGet(() -> tr.get(versionKey).thenApply(SplitHelper::unpackVersion));
        final CompletableFuture<byte[]> unsplitValueFuture = tr.get(unsplitKey);
        final CompletableFuture<byte[]> startSplitValueFuture = tr.get(startSplitKey);

        return versionValueFuture.thenCompose(version -> unsplitValueFuture.thenCombine(startSplitValueFuture, (unsplitValue, startSplitValue) -> {
            storedSizes.add(versionKey, version);
            if (unsplitValue == null && startSplitValue == null) {
                // No record present
                if (version != null) {
                    throw new FoundSplitWithoutStartException(SplitHelper.RECORD_VERSION, false)
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(recordSubspace.pack()))
                            .addLogInfo(LogMessageKeys.VERSION, version);
                }
                return CompletableFuture.completedFuture((FDBRawRecord)null);
            } else if (unsplitValue != null && startSplitValue != null) {
                throw new RecordCoreException("Unsplit value followed by split.")
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(recordSubspace.pack()));
            } else if (unsplitValue != null) {
                // Record is unsplit. No further scans needed
                storedSizes.setSplit(false);
                storedSizes.add(unsplitKey, unsplitValue);
                return CompletableFuture.completedFuture(new FDBRawRecord(key, unsplitValue, version, storedSizes));
            } else {
                // Record is split. Do a scan for the rest of the keys
                storedSizes.setSplit(true);
                storedSizes.add(startSplitKey, startSplitValue);
                final AsyncIterable<KeyValue> iterable = scanSingleRecord(tr, recordSubspace.pack(START_SPLIT_RECORD + 1L), recordRange.end);
                List<byte[]> values = new ArrayList<>();
                values.add(startSplitValue);

                AtomicLong lastSplit = new AtomicLong(START_SPLIT_RECORD);
                return AsyncUtil.forEach(iterable, keyValue -> {
                    long splitPoint = recordSubspace.unpack(keyValue.getKey()).getLong(0);
                    long expectedSplit = lastSplit.incrementAndGet();
                    if (splitPoint != expectedSplit) {
                        throw new RecordCoreException("Split record segments out of order")
                                .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(keyValue.getKey()))
                                .addLogInfo(LogMessageKeys.EXPECTED_INDEX, expectedSplit)
                                .addLogInfo(LogMessageKeys.FOUND_INDEX, splitPoint)
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                    }
                    storedSizes.add(keyValue);
                    values.add(keyValue.getValue());
                }, context.getExecutor()).thenApply(vignore -> {
                    byte[] rawRecord = ByteArrayUtil.join(values.toArray(new byte[0][0]));
                    return new FDBRawRecord(key, rawRecord, version, storedSizes);
                });
            }
        })).thenCompose(Function.identity());
    }

    private static AsyncIterable<KeyValue> scanSingleRecord(final ReadTransaction tr, byte[] start, byte[] end) {
        // Scan with WANT_ALL and no row limit because, for a single record, we need to load all of the keys to
        // reconstitute the record
        return tr.getRange(start, end, ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL);
    }

    // Old save behavior prior to SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION
    // Primary keys were not given the UNSPLIT_RECORD suffix in unsplit stores
    private static CompletableFuture<FDBRawRecord> loadUnsplitLegacy(@Nonnull final ReadTransaction tr,
                                                                     @Nonnull final FDBTransactionContext context,
                                                                     @Nonnull final Subspace subspace,
                                                                     @Nonnull final Tuple key,
                                                                     @Nullable SizeInfo sizeInfo) {
        final long startTime = System.nanoTime();
        final byte[] keyBytes = subspace.pack(key);
        return tr.get(keyBytes).thenApply(valueBytes -> {
            if (context.getTimer() != null) {
                context.getTimer().recordSinceNanoTime(FDBStoreTimer.DetailEvents.GET_RECORD_RAW_VALUE, startTime);
            }
            if (valueBytes != null && sizeInfo != null) {
                sizeInfo.set(keyBytes, valueBytes);
            }
            if (valueBytes != null) {
                if (sizeInfo != null) {
                    return new FDBRawRecord(key, valueBytes, null, sizeInfo);
                } else {
                    return new FDBRawRecord(key, valueBytes, null, 1, keyBytes.length, valueBytes.length, false, false);
                }
            } else {
                return null;
            }
        });
    }

    /**
     * Checks to see if a given key exists.
     * @param tr read transaction
     * @param context transaction context
     * @param subspace subspace containing serialized value
     * @param key key within subspace
     * @param splitLongRecords <code>true</code> if multiple keys should be used; if <code>false</code>, <code>serialized</code> must fit in a single key
     * @param missingUnsplitRecordSuffix if <code>splitLongRecords</code> is <code>false</code> and this is <code>true</code>, this will assume keys are missing a suffix for backwards compatibility reasons
     * @return <code>true</code> if the provided key exists, false otherwise.
     */
    public static CompletableFuture<Boolean> keyExists(@Nonnull final ReadTransaction tr,
                                                       @Nonnull final FDBTransactionContext context,
                                                       @Nonnull final Subspace subspace,
                                                       @Nonnull final Tuple key,
                                                       final boolean splitLongRecords,
                                                       boolean missingUnsplitRecordSuffix) {
        if (!splitLongRecords && missingUnsplitRecordSuffix) {
            return loadUnsplitLegacy(tr, context, subspace, key, null)
                    .thenApply(Objects::nonNull);
        }

        final long startTime = System.nanoTime();
        final byte[] keyBytes = subspace.pack(key);
        final AsyncIterable<KeyValue> rangeScan = tr.getRange(Range.startsWith(keyBytes), 1);
        return context.instrument(FDBStoreTimer.DetailEvents.GET_RECORD_RANGE_RAW_FIRST_CHUNK,
                rangeScan.iterator().onHasNext(), startTime);
    }

    /**
     * Delete the serialized representation of a record. This possibly requires deleting multiple keys, but this method makes
     * an effort to clear only as much as needed.
     * @param context write transaction
     * @param subspace subspace to delete from
     * @param key primary key of the record to delete within <code>subspace</code>
     * @param splitLongRecords <code>true</code> if multiple keys should be used; if <code>false</code>, <code>serialized</code> must fit in a single key
     * @param missingUnsplitRecordSuffix if <code>splitLongRecords</code> is <code>false</code> and this is <code>true</code>, this will assume keys are missing a suffix for backwards compatibility reasons
     * @param clearBasedOnPreviousSizeInfo if <code>splitLongRecords</code>, whether to use <code>previousSizeInfo</code> to determine how much to clear
     * @param previousSizeInfo if <code>clearBasedOnPreviousSizeInfo</code>, the {@link FDBStoredSizes} for any old record, or <code>null</code> if there was no old record
     */
    public static void deleteSplit(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace, @Nonnull final Tuple key,
                                   final boolean splitLongRecords, final boolean missingUnsplitRecordSuffix,
                                   final boolean clearBasedOnPreviousSizeInfo, @Nullable final FDBStoredSizes previousSizeInfo) {
        if (!splitLongRecords && missingUnsplitRecordSuffix) {
            context.ensureActive().clear(subspace.pack(key));
        } else {
            clearPreviousSplitRecord(context, subspace, key, clearBasedOnPreviousSizeInfo, previousSizeInfo);
        }
    }

    public static Tuple unpackKey(@Nonnull Subspace subspace, @Nonnull KeyValue kv) {
        try {
            return subspace.unpack(kv.getKey());
        } catch (IllegalArgumentException e) {
            throw new RecordCoreArgumentException("unable to unpack key", e)
                    .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                    .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()));
        }
    }

    /**
     * A utility to ensure the given key has a valid suffix.
     * Suffix has to be a Long, and within the range of supported suffixes.
     * @param keyTuple the key tuple to validate
     */
    public static void validatePrimaryKeySuffixNumber(final Tuple keyTuple) {
        long nextIndex;
        try {
            nextIndex = keyTuple.getLong(keyTuple.size() - 1);
        } catch (Exception e) {
            throw new RecordCoreStorageException("Invalid record split number: not a number", e);
        }
        // Some validation of the index to match known split enumerators
        if ((nextIndex != RECORD_VERSION) && (nextIndex != UNSPLIT_RECORD) && !(nextIndex >= START_SPLIT_RECORD)) {
            throw new RecordCoreStorageException("Invalid record split number: ")
                    .addLogInfo(LogMessageKeys.SPLIT_NEXT_INDEX,  nextIndex);
        }
    }

    /**
     * Accumulator for key-value sizes while loading / saving split records.
     */
    public static class SizeInfo implements FDBStoredSizes {
        private int keyCount;
        private int keySize;
        private int valueSize;
        private boolean split;
        private boolean versionedInline;

        @Override
        public int getKeyCount() {
            return keyCount;
        }

        public void setKeyCount(int keyCount) {
            this.keyCount = keyCount;
        }

        @Override
        public int getKeySize() {
            return keySize;
        }

        public void setKeySize(int keySize) {
            this.keySize = keySize;
        }

        @Override
        public int getValueSize() {
            return valueSize;
        }

        public void setValueSize(int valueSize) {
            this.valueSize = valueSize;
        }

        @Override
        public boolean isSplit() {
            return split;
        }

        public void setSplit(boolean split) {
            this.split = split;
        }

        @Override
        public boolean isVersionedInline() {
            return versionedInline;
        }

        public void setVersionedInline(boolean versionedInline) {
            this.versionedInline = versionedInline;
        }

        public void set(@Nonnull final KeyValue keyValue) {
            set(keyValue.getKey(), keyValue.getValue());
        }

        public void set(@Nonnull final byte[] keyBytes, @Nonnull final byte[] valueBytes) {
            keyCount = 1;
            keySize = keyBytes.length;
            valueSize = valueBytes.length;
        }

        public void add(@Nonnull final KeyValue keyValue) {
            add(keyValue.getKey(), keyValue.getValue());
        }

        public void add(@Nonnull final byte[] keyBytes, @Nonnull final byte[] valueBytes) {
            keyCount += 1;
            keySize += keyBytes.length;
            valueSize += valueBytes.length;
        }

        public void add(@Nonnull final byte[] keyBytes, @Nullable final FDBRecordVersion version) {
            if (version != null) {
                keyCount += 1;
                keySize += keyBytes.length;
                valueSize += FDBRecordVersion.VERSION_LENGTH + 1;
                versionedInline = true;
            }
        }

        public void add(@Nonnull FDBStoredSizes sizes) {
            keyCount += sizes.getKeyCount();
            keySize += sizes.getKeySize();
            valueSize += sizes.getValueSize();
        }

        public void reset() {
            keyCount = 0;
            keySize = 0;
            valueSize = 0;
            split = false;
            versionedInline = false;
        }
    }

    private static List<Long> offsets(FDBStoredSizes sizeInfo) {
        List<Long> offsetList = new ArrayList<>(sizeInfo.getKeyCount());
        if (sizeInfo.isVersionedInline()) {
            offsetList.add(RECORD_VERSION);
        }
        if (sizeInfo.isSplit()) {
            long current = START_SPLIT_RECORD;
            while (offsetList.size() < sizeInfo.getKeyCount()) {
                offsetList.add(current);
                current++;
            }
        } else {
            offsetList.add(UNSPLIT_RECORD);
        }
        return offsetList;
    }

    /**
     * Unsplit a single record from a given range scan.
     */
    // TODO: The alternative is to use streams throughout the serialization pipeline, from
    //  a range scan through to decryption and Protobuf coded input.
    public static class SingleKeyUnsplitter {
        @Nonnull
        private final FDBRecordContext context;
        @Nonnull
        private final Tuple key;
        @Nonnull
        private final Subspace keySplitSubspace;
        @Nonnull
        private final SizeInfo sizeInfo;
        @Nonnull
        private final AsyncIterator<KeyValue> iter;
        private long lastIndex;
        @Nullable
        private byte[] result;
        @Nullable
        private FDBRecordVersion version;

        public SingleKeyUnsplitter(@Nonnull FDBRecordContext context, @Nonnull Tuple key,
                                   @Nonnull final Subspace keySplitSubspace,
                                   @Nonnull final AsyncIterator<KeyValue> iter, @Nullable final SizeInfo sizeInfo) {
            this.context = context;
            this.key = key;
            this.keySplitSubspace = keySplitSubspace;
            this.iter = iter;
            this.sizeInfo = sizeInfo == null ? new SizeInfo() : sizeInfo;
        }

        /**
         * Unsplit a record in the database.
         * @param executor the executor to use for running asynchronous code
         * @return a future with the raw bytes of the values concatenated back together
         * or {@code null} if the underlying iterator has no items or if the {@code KeyValue} is not split
         * and its value is {@code null}
         */
        @Nonnull
        public CompletableFuture<FDBRawRecord> run(Executor executor) {
            sizeInfo.reset();
            final byte[] versionKey = keySplitSubspace.pack(RECORD_VERSION);
            context.getLocalVersion(versionKey).ifPresent(localVersion -> {
                version = FDBRecordVersion.incomplete(localVersion);
                sizeInfo.add(versionKey, version);
            });
            return AsyncUtil.whileTrue(() -> iter.onHasNext()
                .thenApply(hasNext -> {
                    if (hasNext) {
                        append(iter.next());
                    }
                    return hasNext;
                }), executor).thenApply(vignore -> {
                    if (result != null) {
                        return new FDBRawRecord(key, result, version, sizeInfo);
                    } else if (version != null) {
                        throw new FoundSplitWithoutStartException(SplitHelper.RECORD_VERSION, false)
                                .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()))
                                .addLogInfo(LogMessageKeys.VERSION, version);

                    } else {
                        return null;
                    }
                });
        }

        protected void append(@Nonnull final KeyValue kv) {
            final Tuple subkey = unpackKey(keySplitSubspace, kv);
            if (subkey.size() != 1) {
                throw new RecordCoreException("Expected only a single key extension for split record.")
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()));
            }
            long index = subkey.getLong(0);
            if (index == UNSPLIT_RECORD) {
                if (result != null) {
                    throw new RecordCoreException("More than one unsplit value.")
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()));
                }
                result = kv.getValue();
                sizeInfo.add(kv);
                sizeInfo.setSplit(false);
            } else if (index == lastIndex + 1 || (lastIndex == RECORD_VERSION && index == START_SPLIT_RECORD)) {
                if (index == START_SPLIT_RECORD) {
                    if (result != null) {
                        throw new RecordCoreException("Unsplit value followed by split.")
                                .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()));
                    }
                    result = kv.getValue();
                    sizeInfo.add(kv);
                    sizeInfo.setSplit(true);
                } else {
                    // TODO: ByteArrayOutputStream would grow better?
                    result = ByteArrayUtil.join(result, kv.getValue());
                    sizeInfo.add(kv);
                }
                lastIndex = index;
            } else if (index == RECORD_VERSION) {
                version = unpackVersion(kv.getValue());
                sizeInfo.setVersionedInline(true);
                sizeInfo.add(kv);
            } else {
                if (lastIndex >= SplitHelper.START_SPLIT_RECORD) {
                    throw new FoundSplitOutOfOrderException(lastIndex + 1, index)
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()));
                } else {
                    throw new FoundSplitWithoutStartException(index, false)
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, key)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(keySplitSubspace.pack()));
                }
            }
        }
    }

    /**
     * This cursor may exceed out-of-band limits in order to ensure that it only ever stops in between (split) records.
     * It is therefore unusual since it may exceed the record scan limit arbitrarily based on the size of the record
     * and the maximum value size.
     */
    public static class KeyValueUnsplitter implements BaseCursor<FDBRawRecord> {
        @Nonnull
        private final FDBRecordContext context;
        @Nonnull
        private final RecordCursor<KeyValue> inner;
        private final boolean oldVersionFormat;
        @Nonnull
        private final SizeInfo sizeInfo;
        private final boolean reverse;
        @Nonnull
        private final Subspace subspace;
        @Nullable
        private KeyValue next;
        @Nullable
        private Tuple nextKey;
        @Nullable
        private Subspace nextSubspace;
        @Nullable
        private FDBRecordVersion nextVersion;
        @Nullable
        private byte[] nextPrefix;
        private long nextIndex;
        @Nullable
        private NoNextReason innerNoNextReason;
        @Nullable
        private RecordCursorResult<KeyValue> pending;
        @Nullable
        private RecordCursorContinuation continuation;
        @Nonnull
        private final CursorLimitManager limitManager;
        private long readLastKeyNanos = 0L; // for logging purposes

        // for supporting old cursor API
        @Nullable
        private RecordCursorResult<FDBRawRecord> nextResult;

        public KeyValueUnsplitter(@Nonnull FDBRecordContext context, @Nonnull final Subspace subspace,
                                  @Nonnull final RecordCursor<KeyValue> inner, final boolean oldVersionFormat,
                                  @Nullable final SizeInfo sizeInfo, @Nonnull ScanProperties scanProperties) {
            this(context, subspace, inner, oldVersionFormat, sizeInfo, scanProperties.isReverse(), new CursorLimitManager(scanProperties));
        }

        public KeyValueUnsplitter(@Nonnull FDBRecordContext context, @Nonnull final Subspace subspace,
                                  @Nonnull final RecordCursor<KeyValue> inner, final boolean oldVersionFormat,
                                  @Nullable final SizeInfo sizeInfo,
                                  boolean reverse, @Nonnull CursorLimitManager limitManager) {
            this.context = context;
            this.subspace = subspace;
            this.inner = inner;
            this.oldVersionFormat = oldVersionFormat;
            this.sizeInfo = sizeInfo == null ? new SizeInfo() : sizeInfo;
            this.reverse = reverse;
            this.limitManager = limitManager;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<FDBRawRecord>> onNext() {
            if (nextResult != null && !nextResult.hasNext()) {
                return CompletableFuture.completedFuture(nextResult);
            }
            if (limitManager.isStopped()) {
                final NoNextReason noNextReason = mergeNoNextReason();
                if (noNextReason.isSourceExhausted()) {
                    // Can happen if the limit is reached while reading the final record, so the inner cursor
                    // completes with SOURCE_EXHAUSTED
                    nextResult = RecordCursorResult.exhausted();
                } else {
                    nextResult = RecordCursorResult.withoutNextValue(continuation, mergeNoNextReason());
                }
                return CompletableFuture.completedFuture(nextResult);
            } else {
                return appendUntilNewKey().thenApply(vignore -> {
                    if (nextVersion != null && next == null) {
                        throw new FoundSplitWithoutStartException(RECORD_VERSION, reverse)
                                .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()))
                                .addLogInfo(LogMessageKeys.VERSION, nextVersion);
                    }
                    if (!oldVersionFormat && nextKey != null) {
                        // Account for incomplete version
                        final byte[] versionKey = subspace.subspace(nextKey).pack(RECORD_VERSION);
                        context.getLocalVersion(versionKey).ifPresent(localVersion -> {
                            nextVersion = FDBRecordVersion.incomplete(localVersion);
                            sizeInfo.setVersionedInline(true);
                            sizeInfo.keyCount += 1;
                            sizeInfo.keySize += versionKey.length;
                            sizeInfo.valueSize += 1 + FDBRecordVersion.VERSION_LENGTH;
                        });
                    }

                    if (next == null) { // no next result
                        nextResult = RecordCursorResult.withoutNextValue(continuation, mergeNoNextReason());
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(KeyValueLogMessage.of("unsplitter stopped",
                                    LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                    LogMessageKeys.NO_NEXT_REASON, nextResult.getNoNextReason(),
                                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey())));
                        }
                    } else { // has next result
                        sizeInfo.setVersionedInline(nextVersion != null);
                        final FDBRawRecord result = new FDBRawRecord(nextKey, next.getValue(), nextVersion, sizeInfo);
                        next = null;
                        nextKey = null;
                        nextVersion = null;
                        nextPrefix = null;
                        nextResult =  RecordCursorResult.withNextValue(result, continuation);
                        if (LOGGER.isTraceEnabled()) {
                            KeyValueLogMessage msg = KeyValueLogMessage.build("unsplitter assembled new record",
                                    LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                    LogMessageKeys.KEY_TUPLE, result.getPrimaryKey(),
                                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()));
                            result.addSizeLogInfo(msg);
                            LOGGER.trace(msg.toString());
                        }
                    }
                    return nextResult;
                });
            }
        }

        @Nonnull
        @Override
        public RecordCursorResult<FDBRawRecord> getNext() {
            return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, onNext());
        }

        @Nonnull
        public NoNextReason mergeNoNextReason() {
            if (innerNoNextReason == NoNextReason.SOURCE_EXHAUSTED) {
                return innerNoNextReason;
            }
            return limitManager.getStoppedReason().orElse(innerNoNextReason);
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public boolean isClosed() {
            return inner.isClosed();
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return inner.getExecutor();
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            if (visitor.visitEnter(this)) {
                inner.accept(visitor);
            }
            return visitor.visitLeave(this);
        }

        // Process all elements from the scan until we get a new primary key
        @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn") // Name and negation make it much clearer as is
        private CompletableFuture<Void> appendUntilNewKey() {
            return AsyncUtil.whileTrue(() -> {
                if (pending != null) {
                    boolean complete = append(pending);
                    pending = null;
                    if (complete) {
                        // Split followed by unsplit; available right away.
                        return AsyncUtil.READY_FALSE;
                    }
                }
                return inner.onNext().thenApply(innerResult -> {
                    if (!innerResult.hasNext()) {
                        if (reverse && next != null && nextIndex != START_SPLIT_RECORD && nextIndex != UNSPLIT_RECORD && nextIndex != RECORD_VERSION) {
                            throw new FoundSplitWithoutStartException(nextIndex, true)
                                    .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                                    .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                        }
                        innerNoNextReason = innerResult.getNoNextReason();
                        // If we already built up some values, then we already cached an appropriate continuation.
                        // If we haven't the continuation might have changed so we need to refresh it.
                        if (next == null) {
                            continuation = innerResult.getContinuation();
                        }
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(KeyValueLogMessage.of("unsplitter inner cursor stopped",
                                    LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                    LogMessageKeys.NO_NEXT_REASON, innerNoNextReason,
                                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey())
                            ));
                        }
                        return false;
                    } else {
                        innerNoNextReason = null; // currently, we have a next value
                        limitManager.tryRecordScan();
                        boolean complete = append(innerResult);
                        return !complete;
                    }
                });
            }, inner.getExecutor());
        }

        // Process the next key-value pair from the inner cursor; return whether unsplit complete.
        protected boolean append(@Nonnull RecordCursorResult<KeyValue> resultWithKv) {
            @Nonnull KeyValue kv = resultWithKv.get(); // KeyValue is non-null since we only pass in a result that has one
            limitManager.reportScannedBytes(kv.getKey().length + kv.getValue().length);
            if (nextPrefix == null) {
                continuation = resultWithKv.getContinuation();
                return appendFirst(kv);
            } else if (ByteArrayUtil.startsWith(kv.getKey(), nextPrefix)) {
                continuation = resultWithKv.getContinuation();
                return appendNext(kv);
            } else {
                if (reverse && nextIndex != UNSPLIT_RECORD && nextIndex != START_SPLIT_RECORD && nextIndex != RECORD_VERSION) {
                    throw new FoundSplitWithoutStartException(nextIndex, true)
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey) // nextKey may be null if no version and scanning in the forward direction
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                }
                pending = resultWithKv;
                logEndFound();
                return true;
            }
        }

        // Process the first key-value pair for a given record; return whether the record is complete
        private boolean appendFirst(@Nonnull KeyValue kv) {
            final Tuple keyTuple = subspace.unpack(kv.getKey());
            nextKey = keyTuple.popBack(); // Remove index item
            nextSubspace = subspace.subspace(nextKey);
            nextPrefix = nextSubspace.pack();
            next = new KeyValue(nextPrefix, kv.getValue());
            nextIndex = keyTuple.getLong(keyTuple.size() - 1);
            sizeInfo.set(kv);
            boolean done;
            if (nextIndex == UNSPLIT_RECORD) {
                // First key is an unsplit record key. Either this is going
                // in the forward direction (in which case this is the only
                // key), or we are going in the reverse direction, in which
                // case there might be a version key before it.
                sizeInfo.setSplit(false);
                done = !reverse;
            } else if (!reverse && nextIndex == RECORD_VERSION) {
                if (oldVersionFormat) {
                    throw new RecordCoreException("Found record version when old format specified")
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
                }
                // First key is a record version. This should only happen in
                // the forward scan direction, so if this happens in
                // the reverse direction, it means that there isn't any
                // data associated with the record, but there is a version.
                sizeInfo.setVersionedInline(true);
                nextVersion = unpackVersion(kv.getValue());
                next = null;
                done = false;
            } else if (reverse && nextIndex != RECORD_VERSION || nextIndex == START_SPLIT_RECORD) {
                // The data is either the beginning or end of the split (depending
                // on scan direction).
                sizeInfo.setSplit(true);
                done = false;
            } else {
                throw new FoundSplitWithoutStartException(nextIndex, reverse)
                        .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
            }
            logFirstKey(done);
            return done;
        }

        // Process the a key-value pair (other than the first one) for a given record; return whether the record is complete
        private boolean appendNext(@Nonnull KeyValue kv) {
            long index = nextSubspace.unpack(kv.getKey()).getLong(0);
            sizeInfo.add(kv);
            boolean done;
            if (!reverse && nextIndex == RECORD_VERSION && (index == UNSPLIT_RECORD || index == START_SPLIT_RECORD)) {
                // The first key (in a forward) scan was a version. Set the key (so far) to be
                // just what has been read from this key. If it is the beginning of
                // a split record, we have more to do. Otherwise, we know this is the
                // end of the record.
                next = new KeyValue(nextPrefix, kv.getValue());
                nextIndex = index;
                sizeInfo.setSplit(index == START_SPLIT_RECORD);
                done = nextIndex == UNSPLIT_RECORD;
            } else if (!reverse && index == nextIndex + 1) {
                // This is the second or later key (not counting a possible version key)
                // in the forward scan. Append its value to the end of the current
                // key-value pair being accumulated. Return false because there is
                // no way to know if this is the last key or not.
                next = new KeyValue(nextPrefix, ByteArrayUtil.join(next.getValue(), kv.getValue()));
                nextIndex = index;
                done = false;
            } else if (reverse && index == RECORD_VERSION && (nextIndex == START_SPLIT_RECORD || nextIndex == UNSPLIT_RECORD)) {
                // This is the record version key encountered during a backwards scan.
                // Update the version information and return true, as the record version
                // is always first key for a given record (if it is present).
                if (oldVersionFormat) {
                    throw new RecordCoreException("Found record version when old format specified")
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                }
                nextVersion = unpackVersion(kv.getValue());
                nextIndex = index;
                done = true;
            } else if (reverse && index == nextIndex - 1 && index != RECORD_VERSION) {
                // The second or later key in a backwards scan, but not the record version.
                // Append its value to the beginning of the current key-value pair being
                // accumulated. Return false because there is no way to know if this is the
                // last key or not (in particular, even if index == START_SPLIT_RECORD, it's
                // possible that there is a record version before it).
                next = new KeyValue(nextPrefix, ByteArrayUtil.join(kv.getValue(), next.getValue()));
                nextIndex = index;
                done = false;
            } else {
                final long expectedIndex = nextIndex + (reverse ? -1 : 1);
                if (reverse && expectedIndex == START_SPLIT_RECORD || !reverse && nextIndex == RECORD_VERSION) {
                    throw new FoundSplitWithoutStartException(index, reverse)
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                } else {
                    throw new RecordCoreException("Split record segments out of order")
                            .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                            .addLogInfo(LogMessageKeys.EXPECTED_INDEX, nextIndex + (reverse ? -1 : 1))
                            .addLogInfo(LogMessageKeys.FOUND_INDEX, index)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
                }
            }
            logNextKey(done);
            return done;
        }

        private void logFirstKey(boolean done) {
            logKey("found first key in new split record", done);
        }

        private void logNextKey(boolean done) {
            logKey("found next key in split record", done);
        }

        private void logEndFound() {
            logKey("end key found for split record", true);
        }

        private void logKey(@Nonnull String staticMessage, boolean done) {
            if (LOGGER.isTraceEnabled()) {
                KeyValueLogMessage msg = KeyValueLogMessage.build(staticMessage,
                        LogMessageKeys.KEY_TUPLE, nextKey,
                        LogMessageKeys.SPLIT_REVERSE, reverse,
                        LogMessageKeys.SPLIT_NEXT_INDEX, nextIndex,
                        LogMessageKeys.KNOWN_LAST_KEY, done,
                        LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()));
                sizeInfo.addSizeLogInfo(msg);
                long currentNanos = System.nanoTime();
                if (readLastKeyNanos != 0) {
                    msg.addKeyAndValue(LogMessageKeys.READ_LAST_KEY_MICROS, TimeUnit.NANOSECONDS.toMicros(currentNanos - readLastKeyNanos));
                }
                readLastKeyNanos = currentNanos;
                LOGGER.trace(msg.toString());
            }
        }
    }

    /**
     * Exception thrown when only part of a split record is found.
     */
    @SuppressWarnings("serial")
    public static final class FoundSplitWithoutStartException extends RecordCoreException {
        public FoundSplitWithoutStartException(long nextIndex, boolean reverse) {
            super("Found split record without start");
            addLogInfo(LogMessageKeys.SPLIT_NEXT_INDEX, nextIndex);
            addLogInfo(LogMessageKeys.SPLIT_REVERSE, reverse);
        }
    }

    /**
     * Exception thrown when splits are out of order.
     */
    @SuppressWarnings("serial")
    public static final class FoundSplitOutOfOrderException extends RecordCoreStorageException {
        public FoundSplitOutOfOrderException(long expected, long found) {
            super("Split record segments out of order");
            addLogInfo(LogMessageKeys.SPLIT_EXPECTED, expected);
            addLogInfo(LogMessageKeys.SPLIT_FOUND, found);
        }
    }
}

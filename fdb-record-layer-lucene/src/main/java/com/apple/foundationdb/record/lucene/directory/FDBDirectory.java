/*
 * FDBDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndexV1;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndexV2;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedFieldInfosFormat;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedStoredFieldsFormat;
import com.apple.foundationdb.record.lucene.codec.PrefetchableBufferedChecksumIndexInput;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.util.pair.ComparablePair;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCompoundFormat.DATA_EXTENSION;
import static com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCompoundFormat.ENTRIES_EXTENSION;
import static org.apache.lucene.codecs.lucene86.Lucene86SegmentInfoFormat.SI_EXTENSION;

/**
 * Directory implementation backed by FDB which attempts to
 * model a file system on top of FoundationDB.
 *
 * A few interesting details:
 *
 * <ul>
 *     <li>Each segment written in Lucene once flushed is <b>immutable</b>.</li>
 *     <li>As more data is written into Lucene, files are continually merged via a <a href="https://lucene.apache.org/core/7_6_0/core/org/apache/lucene/index/MergePolicy.html">MergePolicy</a></li>
 * </ul>
 *
 * @see <a href="https://lucene.apache.org/core/7_6_0/core/org/apache/lucene/store/Directory.html">Directory</a>
 */
@API(API.Status.EXPERIMENTAL)
@NotThreadSafe
public class FDBDirectory extends Directory  {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectory.class);
    public static final int DEFAULT_BLOCK_SIZE = 1_024;
    public static final int DEFAULT_BLOCK_CACHE_MAXIMUM_SIZE = 1024;
    public static final int DEFAULT_CONCURRENCY_LEVEL = 16;
    public static final int DEFAULT_INITIAL_CAPACITY = 128;
    private static final int SEQUENCE_SUBSPACE = 0;
    private static final int META_SUBSPACE = 1;
    private static final int DATA_SUBSPACE = 2;
    @SuppressWarnings({"Unused", "PMD.UnusedPrivateField"}) // preserved to document that this is reserved
    private static final int SCHEMA_SUBSPACE = 3;
    private static final int PRIMARY_KEY_SUBSPACE = 4;
    private static final int FIELD_INFOS_SUBSPACE = 5;
    private static final int STORED_FIELDS_SUBSPACE = 6;
    @VisibleForTesting
    public static final int FILE_LOCK_SUBSPACE = 7;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    @Nonnull
    private final Map<String, String> indexOptions;
    private final Subspace subspace;
    private final Subspace metaSubspace;
    private final Subspace dataSubspace;
    private final Subspace fieldInfosSubspace;
    protected final Subspace storedFieldsSubspace;
    private final Subspace fileLockSubspace;
    private final byte[] sequenceSubspaceKey;

    private final FDBDirectoryLockFactory lockFactory;
    private FDBDirectoryLockFactory.FDBDirectoryLock lastLock = null;
    private final int blockSize;

    /**
     * A supplier used to load the {@link #fileReferenceCache}. The supplier allows us to memoize listing all
     * the files from the database so that we don't have to do that more than once per {@link FDBDirectory}
     * per transaction.
     */
    private final Supplier<CompletableFuture<Void>> fileReferenceMapSupplier;
    /**
     * A cached map from file names to file meta-data (such as the file size and ID).
     * The file reference cache should be loaded by {@link #getFileReferenceCache()} or {@link #getFileReferenceCacheAsync()}.
     * Note that this is a skip-list map instead of a hash map because key iteration order is important:
     * the {@link #listAll()} method requires that file names be returned in sorted order.
     * @see FDBLuceneFileReference
     */
    private final AtomicReference<ConcurrentSkipListMap<String, FDBLuceneFileReference>> fileReferenceCache;
    private final FieldInfosStorage fieldInfosStorage;

    private final AtomicLong fileSequenceCounter;

    private final Cache<ComparablePair<Long, Integer>, CompletableFuture<byte[]>> blockCache;

    private final boolean compressionEnabled;
    private final boolean encryptionEnabled;

    // The shared cache is initialized when first listing the directory, if a manager is present, and cleared before writing.
    @Nullable
    private final FDBDirectorySharedCacheManager sharedCacheManager;
    @Nullable
    private final Tuple sharedCacheKey;
    // Whether to delete K/V data immediately or wait for compound file deletion
    private final boolean deferDeleteToCompoundFile;
    @Nullable
    private FDBDirectorySharedCache sharedCache;
    // True if sharedCacheManager is present until sharedCache has been set (or not).
    private boolean sharedCachePending;
    private final AgilityContext agilityContext;

    @Nullable
    private LucenePrimaryKeySegmentIndex primaryKeySegmentIndex;

    @VisibleForTesting
    public FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nullable Map<String, String> indexOptions) {
        this(subspace, indexOptions, null, null, true, AgilityContext.nonAgile(context));
    }

    public FDBDirectory(@Nonnull Subspace subspace, @Nullable Map<String, String> indexOptions,
                        @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey,
                        boolean deferDeleteToCompoundFile, AgilityContext agilityContext) {
        this(subspace, indexOptions, sharedCacheManager, sharedCacheKey, agilityContext,
                DEFAULT_BLOCK_SIZE, DEFAULT_INITIAL_CAPACITY, DEFAULT_BLOCK_CACHE_MAXIMUM_SIZE, DEFAULT_CONCURRENCY_LEVEL, deferDeleteToCompoundFile);
    }

    public FDBDirectory(@Nonnull Subspace subspace, @Nullable Map<String, String> indexOptions,
                        @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey,
                        boolean deferDeleteToCompoundFile, AgilityContext agilityContext, int blockCacheMaximumSize) {
        this(subspace, indexOptions, sharedCacheManager, sharedCacheKey, agilityContext,
                DEFAULT_BLOCK_SIZE, DEFAULT_INITIAL_CAPACITY, blockCacheMaximumSize, DEFAULT_CONCURRENCY_LEVEL, deferDeleteToCompoundFile);
    }

    private FDBDirectory(@Nonnull Subspace subspace, @Nullable Map<String, String> indexOptions,
                         @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey, AgilityContext agilityContext,
                         int blockSize, final int initialCapacity, final int blockCacheMaximumSize, final int concurrencyLevel,
                         boolean deferDeleteToCompoundFile) {
        this.agilityContext = agilityContext;
        this.indexOptions = indexOptions == null ? Collections.emptyMap() : indexOptions;
        this.subspace = subspace;
        final Subspace sequenceSubspace = subspace.subspace(Tuple.from(SEQUENCE_SUBSPACE));
        this.sequenceSubspaceKey = sequenceSubspace.pack();
        this.metaSubspace = subspace.subspace(Tuple.from(META_SUBSPACE));
        this.dataSubspace = subspace.subspace(Tuple.from(DATA_SUBSPACE));
        this.fieldInfosSubspace = subspace.subspace(Tuple.from(FIELD_INFOS_SUBSPACE));
        this.storedFieldsSubspace = subspace.subspace(Tuple.from(STORED_FIELDS_SUBSPACE));
        this.fileLockSubspace = subspace.subspace(Tuple.from(FILE_LOCK_SUBSPACE));
        this.lockFactory = new FDBDirectoryLockFactory(this, Objects.requireNonNullElse(agilityContext.getPropertyValue(LuceneRecordContextProperties.LUCENE_FILE_LOCK_TIME_WINDOW_MILLISECONDS), 0));
        this.blockSize = blockSize;
        this.fileReferenceCache = new AtomicReference<>();
        this.blockCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(blockCacheMaximumSize)
                .recordStats()
                .removalListener(notification -> cacheRemovalCallback())
                .build();
        this.fileSequenceCounter = new AtomicLong(-1);
        this.compressionEnabled = Objects.requireNonNullElse(agilityContext.getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED), false);
        this.encryptionEnabled = Objects.requireNonNullElse(agilityContext.getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_ENCRYPTION_ENABLED), false);
        this.fileReferenceMapSupplier = Suppliers.memoize(this::loadFileReferenceCacheForMemoization);
        this.sharedCacheManager = sharedCacheManager;
        this.sharedCacheKey = sharedCacheKey;
        this.sharedCachePending = sharedCacheManager != null && sharedCacheKey != null;
        this.fieldInfosStorage = new FieldInfosStorage(this);
        this.deferDeleteToCompoundFile = deferDeleteToCompoundFile;
    }

    private void cacheRemovalCallback() {
        agilityContext.increment(LuceneEvents.Counts.LUCENE_BLOCK_CACHE_REMOVE);
    }

    private long deserializeFileSequenceCounter(@Nullable byte[] value) {
        return value == null ? 0L : Tuple.fromBytes(value).getLong(0);
    }

    @Nonnull
    private byte[] serializeFileSequenceCounter(long value) {
        return Tuple.from(value).pack();
    }

    @Nonnull
    private CompletableFuture<Void> loadFileSequenceCounter() {
        long originalValue = fileSequenceCounter.get();
        if (originalValue >= 0) {
            // Already loaded
            return AsyncUtil.DONE;
        }
        return agilityContext.get(sequenceSubspaceKey).thenAccept(serializedValue -> {
            // Replace the counter value in the counter unless something else has already
            // updated it, in which case we want to keep the cached value
            long loadedValue = deserializeFileSequenceCounter(serializedValue);
            fileSequenceCounter.compareAndSet(originalValue, loadedValue);
        });
    }

    /**
     * Sets increment if not set yet and waits till completed to return
     * Returns and increments the increment if its already set.
     * @return current increment value
     */
    public long getIncrement() {
        // Stop using any shared cache.
        sharedCachePending = false;
        sharedCache = null;

        agilityContext.increment(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS);

        // Make sure the file counter is loaded from the database into the fileSequenceCounter, then
        // use the cached value
        asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_INCREMENT, loadFileSequenceCounter());
        long incrementedValue = fileSequenceCounter.incrementAndGet();

        // Use BYTE_MAX here so that if there are concurrent calls (which will get different increments because they
        // are serialized around the fileSequenceCounter AtomicLong), the largest one always gets
        // committed into the database
        byte[] serializedValue = serializeFileSequenceCounter(incrementedValue);
        agilityContext.accept(aContext -> aContext.ensureActive().mutate(MutationType.BYTE_MAX, sequenceSubspaceKey, serializedValue));

        return incrementedValue;
    }

    /**
     * Checks the cache for  the file reference.
     * If the file is in the cache it returns the cached value.
     * If not there then it checks the subspace for it.
     * If its there the file is added to the cache and returned to caller
     * If the file doesn't exist in the subspace it returns null.
     *
     * @param name name for the file reference
     * @return FDBLuceneFileReference
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReferenceAsync(@Nonnull final String name) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("getFDBLuceneFileReferenceAsync",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        return getFileReferenceCacheAsync().thenApply(cache -> cache.get(name));
    }

    @API(API.Status.INTERNAL)
    @Nullable
    public FDBLuceneFileReference getFDBLuceneFileReference(@Nonnull final String name) {
        return asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE, getFDBLuceneFileReferenceAsync(name));
    }

    public FieldInfosStorage getFieldInfosStorage() {
        return this.fieldInfosStorage;
    }

    public void setFieldInfoId(final String filename, final long id, final ByteString bitSet) {
        final FDBLuceneFileReference reference = asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE,
                getFDBLuceneFileReferenceAsync(filename));
        if (reference == null) {
            throw new RecordCoreException("Reference not found")
                    .addLogInfo(LuceneLogMessageKeys.FILE_NAME, filename);
        }
        reference.setFieldInfosId(id);
        reference.setFieldInfosBitSet(bitSet);
        writeFDBLuceneFileReference(filename, reference);
    }

    void writeFieldInfos(long id, byte[] value) {
        if (id == 0) {
            throw new RecordCoreArgumentException("FieldInfo id should never be 0");
        }
        byte[] key = fieldInfosSubspace.pack(id);
        agilityContext.recordSize(LuceneEvents.SizeEvents.LUCENE_WRITE, key.length + value.length);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene stored field infos data",
                    LuceneLogMessageKeys.DATA_SIZE, value.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, value.length));
        }
        agilityContext.set(key, value);
    }

    Stream<NonnullPair<Long, byte[]>> getAllFieldInfosStream() {
        return asyncToSync(
                LuceneEvents.Waits.WAIT_LUCENE_READ_FIELD_INFOS,
                agilityContext.apply(aContext -> aContext.ensureActive().getRange(fieldInfosSubspace.range()).asList()))
                .stream()
                .map(keyValue -> NonnullPair.of(fieldInfosSubspace.unpack(keyValue.getKey()).getLong(0), keyValue.getValue()));
    }

    public CompletableFuture<Integer> getFieldInfosCount() {
        return agilityContext.apply(aContext -> aContext.ensureActive().getRange(fieldInfosSubspace.range()).asList())
                .thenApply(List::size);
    }

    public static boolean isSegmentInfo(String name) {
        return name.endsWith(SI_EXTENSION)
               && !name.startsWith(IndexFileNames.SEGMENTS)
               && !name.startsWith(IndexFileNames.PENDING_SEGMENTS);
    }

    public static boolean isCompoundFile(String name) {
        return name.endsWith(DATA_EXTENSION)
               && !name.startsWith(IndexFileNames.SEGMENTS)
               && !name.startsWith(IndexFileNames.PENDING_SEGMENTS);
    }

    public static boolean isEntriesFile(String name) {
        return name.endsWith(ENTRIES_EXTENSION)
               && !name.startsWith(IndexFileNames.SEGMENTS)
               && !name.startsWith(IndexFileNames.PENDING_SEGMENTS);
    }

    public static boolean isFieldInfoFile(String name) {
        return name.endsWith(LuceneOptimizedFieldInfosFormat.EXTENSION)
               && !name.startsWith(IndexFileNames.SEGMENTS)
               && !name.startsWith(IndexFileNames.PENDING_SEGMENTS);
    }

    public static boolean isStoredFieldsFile(String name) {
        return name.endsWith(LuceneOptimizedStoredFieldsFormat.STORED_FIELDS_EXTENSION)
               && !name.startsWith(IndexFileNames.SEGMENTS)
               && !name.startsWith(IndexFileNames.PENDING_SEGMENTS);
    }

    /**
     * Puts a file reference in the meta subspace and in the cache under the given name.
     * @param name name for the file reference
     * @param reference the file reference being inserted
     */
    public void writeFDBLuceneFileReference(@Nonnull String name, @Nonnull FDBLuceneFileReference reference) {
        final byte[] fileReferenceBytes = reference.getBytes();
        final byte[] encodedBytes = Objects.requireNonNull(LuceneSerializer.encode(fileReferenceBytes, compressionEnabled, encryptionEnabled));
        agilityContext.recordSize(LuceneEvents.SizeEvents.LUCENE_WRITE_FILE_REFERENCE, encodedBytes.length);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene file reference",
                    LuceneLogMessageKeys.FILE_NAME, name,
                    LuceneLogMessageKeys.DATA_SIZE, fileReferenceBytes.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encodedBytes.length,
                    LuceneLogMessageKeys.FILE_REFERENCE, reference));
        }
        agilityContext.set(metaSubspace.pack(name), encodedBytes);
        getFileReferenceCache().put(name, reference);
        fieldInfosStorage.addReference(reference);
    }

    /**
     * Writes data to the given block under the given id.
     * @param id id for the data
     * @param block block for the data to be stored in
     * @param value the data to be stored
     * @return the actual data size written to database with potential compression and encryption applied
     */
    public int writeData(final long id, final int block, @Nonnull final byte[] value) {
        final byte[] encodedBytes = Objects.requireNonNull(LuceneSerializer.encode(value, compressionEnabled, encryptionEnabled));
        //This may not be correct transactionally
        agilityContext.recordSize(LuceneEvents.SizeEvents.LUCENE_WRITE, encodedBytes.length);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene data",
                    LuceneLogMessageKeys.FILE_ID, id,
                    LuceneLogMessageKeys.BLOCK_NUMBER, block,
                    LuceneLogMessageKeys.DATA_SIZE, value.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encodedBytes.length));
        }
        Verify.verify(value.length <= blockSize);
        agilityContext.set(dataSubspace.pack(Tuple.from(id, block)), encodedBytes);
        return encodedBytes.length;
    }

    /**
     * Write stored fields document to the DB.
     * @param segmentName the segment name writing to
     * @param docID the document ID to write
     * @param value the bytes value of the stored fields
     */
    public void writeStoredFields(@Nonnull String segmentName, int docID, @Nonnull final byte[] value) {
        byte[] key = storedFieldsSubspace.pack(Tuple.from(segmentName, docID));
        agilityContext.recordSize(LuceneEvents.SizeEvents.LUCENE_WRITE_STORED_FIELDS, key.length + value.length);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene stored fields data",
                    LuceneLogMessageKeys.DATA_SIZE, value.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, value.length));
        }
        agilityContext.set(key, value);
    }

    /**
     * Delete stored fields data from the DB.
     * @param segmentName the segment name to delete the fields from (all docs in the segment will be deleted)
     * @throws IOException if there is an issue reading metadata to do the delete
     */
    public void deleteStoredFields(@Nonnull final String segmentName) throws IOException {
        agilityContext.increment(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Delete Stored Fields Data",
                    LuceneLogMessageKeys.RESOURCE, segmentName));
        }
        final LucenePrimaryKeySegmentIndex primaryKeyIndex = getPrimaryKeySegmentIndex();
        if (primaryKeyIndex != null) {
            primaryKeyIndex.clearForSegment(segmentName);
        }
        byte[] key = storedFieldsSubspace.pack(Tuple.from(segmentName));
        agilityContext.clear(Range.startsWith(key));
    }

    /**
     * Reads known data from the directory.
     * @param requestingInput the {@link FDBIndexInput} requesting the block; used for logging
     * @param fileName Description should be non-null, opaque string describing this resource; used for logging
     * @param referenceFuture the reference where the data supposedly lives
     * @param block the block where the data is stored
     * @return Completable future of the data returned
     * @throws RecordCoreException if blockCache fails to get the data from the block
     * @throws RecordCoreArgumentException if a reference with that id hasn't been written yet.
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public CompletableFuture<byte[]> readBlock(@Nonnull IndexInput requestingInput,
                                               @Nonnull String fileName,
                                               @Nonnull CompletableFuture<FDBLuceneFileReference> referenceFuture,
                                               int block) {
        return referenceFuture.thenCompose(reference -> readBlock(requestingInput, fileName, reference, block));
    }

    @Nonnull
    @SuppressWarnings("PMD.PreserveStackTrace")
    private CompletableFuture<byte[]> readBlock(@Nonnull IndexInput requestingInput, @Nonnull String fileName,
                                                @Nullable FDBLuceneFileReference reference, int block) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("readBlock",
                    LuceneLogMessageKeys.FILE_NAME, fileName,
                    LuceneLogMessageKeys.FILE_REFERENCE, requestingInput,
                    LuceneLogMessageKeys.BLOCK_NUMBER, block));
        }
        if (reference == null) {
            CompletableFuture<byte[]> exceptionalFuture = new CompletableFuture<>();
            exceptionalFuture.completeExceptionally(new RecordCoreArgumentException("No reference with for file name was found").addLogInfo(LogMessageKeys.EXPECTED, fileName));
            return exceptionalFuture;
        }
        final long id = reference.getId();
        try {
            return agilityContext.instrument(LuceneEvents.Events.LUCENE_READ_BLOCK, blockCache.get(ComparablePair.of(id, block), () -> {
                if (sharedCache == null) {
                    return readData(id, block);
                }
                final byte[] fromShared = sharedCache.getBlockIfPresent(id, block);
                if (fromShared != null) {
                    agilityContext.increment(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS);
                    return CompletableFuture.completedFuture(fromShared);
                } else {
                    agilityContext.increment(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES);
                    return readData(id, block).thenApply(data -> {
                        sharedCache.putBlockIfAbsent(id, block, data);
                        return data;
                    });
                }
                    }
            ));
        } catch (ExecutionException e) {
            // This would happen when the cache.get() fails to execute the lambda (not when the block's future is joined)
            throw new RecordCoreException(e.getCause());
        }
    }

    private CompletableFuture<byte[]> readData(long id, int block) {
        return agilityContext.instrument(LuceneEvents.Events.LUCENE_FDB_READ_BLOCK,
                agilityContext.get(dataSubspace.pack(Tuple.from(id, block)))
                        .thenApply(LuceneSerializer::decode));
    }

    @Nonnull
    public byte[] readStoredFields(String segmentName, int docId) throws IOException {
        final byte[] key = storedFieldsSubspace.pack(Tuple.from(segmentName, docId));
        final byte[] rawBytes = asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS,
                agilityContext.instrument(LuceneEvents.Events.LUCENE_READ_STORED_FIELDS,
                        agilityContext.get(key)));
        if (rawBytes == null) {
            throw new RecordCoreStorageException("Could not find stored fields")
                    .addLogInfo(LuceneLogMessageKeys.SEGMENT, segmentName)
                    .addLogInfo(LuceneLogMessageKeys.DOC_ID, docId)
                    .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(key));
        }
        return rawBytes;
    }

    @Nonnull
    public List<KeyValue> readAllStoredFields(String segmentName) {
        final Range range = storedFieldsSubspace.range(Tuple.from(segmentName));
        final List<KeyValue> list = asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_ALL_STORED_FIELDS,
                agilityContext.getRange(range.begin, range.end));
        if (list == null) {
            throw new RecordCoreStorageException("Could not find stored fields")
                    .addLogInfo(LuceneLogMessageKeys.SEGMENT, segmentName)
                    .addLogInfo(LogMessageKeys.RANGE_START, ByteArrayUtil2.loggable(range.begin))
                    .addLogInfo(LogMessageKeys.RANGE_END, ByteArrayUtil2.loggable(range.end));
        }
        return list;
    }

    /**
     * Lists all file names in the subspace. Puts all references in the cache.
     * Logs the count of references, and the total size of the data.
     * All references are put into the cache because each composite file will be required to serve a query and the file references are small.
     *
     * @return String list of names of lucene file references
     */
    @Override
    @Nonnull
    public String[] listAll() {
        long startTime = System.nanoTime();
        try {
            return getFileReferenceCache().keySet().stream().filter(name -> !name.endsWith(".pky")).toArray(String[]::new);
        } finally {
            agilityContext.recordEvent(LuceneEvents.Events.LUCENE_LIST_ALL, System.nanoTime() - startTime);
        }
    }

    public CompletableFuture<Collection<String>> listAllAsync() {
        return getFileReferenceCacheAsync().thenApply(references -> List.copyOf(references.keySet()));
    }

    @VisibleForTesting
    public CompletableFuture<List<KeyValue>> scanStoredFields(String segmentName) {
        return agilityContext.apply(aContext -> aContext.ensureActive()
                .getRange(storedFieldsSubspace.subspace(Tuple.from(segmentName)).range(),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.ITERATOR).asList());
    }

    private CompletableFuture<Void> loadFileReferenceCacheForMemoization() {
        long start = System.nanoTime();
        final ConcurrentSkipListMap<String, FDBLuceneFileReference> outMap = new ConcurrentSkipListMap<>();
        final ConcurrentHashMap<Long, AtomicInteger> fieldInfosCount = new ConcurrentHashMap<>();
        // Issue a range read with StreamingMode.WANT_ALL (instead of default ITERATOR) because this needs to read the
        // entire range before doing anything with the data
        final CompletableFuture<List<KeyValue>> rangeList = agilityContext.apply(aContext -> aContext.ensureActive()
                .getRange(metaSubspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL).asList());
        CompletableFuture<Void> future = rangeList.thenApply(list -> {
            agilityContext.recordSize(LuceneEvents.SizeEvents.LUCENE_FILES_COUNT, list.size());
            list.forEach(kv -> {
                String name = metaSubspace.unpack(kv.getKey()).getString(0);
                final FDBLuceneFileReference fileReference = Objects.requireNonNull(FDBLuceneFileReference.parseFromBytes(LuceneSerializer.decode(kv.getValue())));
                outMap.put(name, fileReference);
                if (fileReference.getFieldInfosId() != 0) {
                    fieldInfosCount.computeIfAbsent(fileReference.getFieldInfosId(), key -> new AtomicInteger(0))
                            .incrementAndGet();
                }
            });
            return null;
        }).thenAccept(ignore -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(fileListLog("listAllFiles", outMap).toString());
            }

            // Memoize the result in an FDBDirectory member variable. Future attempts to access files
            // should use the class variable
            fileReferenceCache.compareAndSet(null, outMap);
            fieldInfosStorage.initializeReferenceCount(fieldInfosCount);
        });
        return agilityContext.instrument(LuceneEvents.Events.LUCENE_LOAD_FILE_CACHE, future, start);
    }

    private KeyValueLogMessage fileListLog(final String listAllFiles,
                                           final Map<String, FDBLuceneFileReference> fileMap) {
        List<String> displayList = new ArrayList<>(fileMap.size());
        long totalSize = 0L;
        long actualTotalSize = 0L;
        for (Map.Entry<String, FDBLuceneFileReference> entry: fileMap.entrySet()) {
            if (displayList.size() < 200 || entry.getKey().startsWith("segments")) {
                displayList.add(entry.getKey());
            }
            totalSize += entry.getValue().getSize();
            actualTotalSize += entry.getValue().getActualSize();
        }
        if (displayList.size() >= 200) {
            displayList.add("...");
        }
        return getKeyValueLogMessage(listAllFiles,
                LuceneLogMessageKeys.FILE_COUNT, fileMap.size(),
                LuceneLogMessageKeys.FILE_LIST, displayList,
                LuceneLogMessageKeys.FILE_TOTAL_SIZE, totalSize,
                LuceneLogMessageKeys.FILE_ACTUAL_TOTAL_SIZE, actualTotalSize);
    }

    @Nonnull
    private CompletableFuture<Map<String, FDBLuceneFileReference>> getFileReferenceCacheAsync() {
        if (fileReferenceCache.get() != null) {
            return CompletableFuture.completedFuture(fileReferenceCache.get());
        }
        if (sharedCachePending) {
            return loadFileSequenceCounter().thenCompose(vignore -> {
                sharedCache = sharedCacheManager.getCache(sharedCacheKey, fileSequenceCounter.get());
                if (sharedCache == null) {
                    sharedCachePending = false;
                    return getFileReferenceCacheAsync();
                }
                Map<String, FDBLuceneFileReference> fromShared = sharedCache.getFileReferencesIfPresent();
                if (fromShared != null) {
                    ConcurrentSkipListMap<String, FDBLuceneFileReference> copy = new ConcurrentSkipListMap<>(fromShared);
                    fileReferenceCache.compareAndSet(null, copy);
                    fieldInfosStorage.initializeReferenceCount(sharedCache.getFieldInfosReferenceCount());
                    sharedCachePending = false;
                    return CompletableFuture.completedFuture(fromShared);
                }
                return fileReferenceMapSupplier.get().thenApply(ignore -> {
                    final ConcurrentSkipListMap<String, FDBLuceneFileReference> fromSupplier = fileReferenceCache.get();
                    sharedCache.setFileReferencesIfAbsent(fromSupplier);
                    sharedCache.setFieldInfosReferenceCount(getFieldInfosStorage().getReferenceCount());
                    sharedCachePending = false;
                    return fromSupplier;
                });
            });
        }
        // Call the supplier to make sure the file reference cache has been loaded into fileReferenceCache, then
        // always return the value that has been memoized in this class
        return fileReferenceMapSupplier.get().thenApply(ignore -> fileReferenceCache.get());
    }

    @Nonnull
    private Map<String, FDBLuceneFileReference> getFileReferenceCache() {
        return Objects.requireNonNull(asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_LOAD_FILE_CACHE, getFileReferenceCacheAsync()));
    }

    /**
     * deletes the file reference under the provided name.
     * @param name the name for the file reference
     */
    @Override
    public void deleteFile(@Nonnull String name) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("deleteFile",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }

        try {
            boolean deleted = deleteFileInternal(
                    Objects.requireNonNull(asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE, getFileReferenceCacheAsync())),
                    name);

            if (!deleted) {
                throw new NoSuchFileException(name);
            }

            if (isCompoundFile(name)) {
                Map<String, FDBLuceneFileReference> cache = this.fileReferenceCache.get();
                String primaryKeyName = name.substring(0, name.length() - DATA_EXTENSION.length()) + "pky";
                deleteFileInternal(cache, primaryKeyName);
                // TODO: If the segment is being deleted because it no longer has any live docs, it won't be merged
                //  so we might need to delete the primary key entries another way for case where tryDeleteDocument
                //  wasn't used.
            }
        } finally {
            agilityContext.increment(LuceneEvents.Counts.LUCENE_DELETE_FILE);
        }
    }

    private boolean deleteFileInternal(@Nonnull Map<String, FDBLuceneFileReference> cache, @Nonnull String name) throws IOException {
        // TODO make this transactional or ensure that it is deleted in the right order
        FDBLuceneFileReference value = cache.remove(name);
        if (value == null) {
            return false;
        }
        final long id = value.getFieldInfosId();
        if (fieldInfosStorage.delete(id)) {
            agilityContext.clear(fieldInfosSubspace.pack(id));
        }
        // Nothing stored here currently.
        agilityContext.clear(dataSubspace.subspace(Tuple.from(id)).range());

        // Delete K/V data: If the deferredDelete flag is on then delete all content from K/V subspace for the segment
        // (this is to support CFS deletion, that will be disjoint from the actual file deletion).
        // Otherwise, delete the data for the specific file immediately.
        String segmentName = IndexFileNames.parseSegmentName(name);
        if (deferDeleteToCompoundFile) {
            if (isCompoundFile(name)) {
                // delete all K/V content, only if the optimized stored fields format is in use
                if (usesOptimizedStoredFields()) {
                    deleteStoredFields(segmentName);
                }
            }
        } else {
            if (isStoredFieldsFile(name)) {
                // Delete stored fields subspace
                deleteStoredFields(segmentName);
            }
        }
        // we want to clear this last, so that if only some of the operations are completed, the reference
        // will stick around, and it will be cleaned up later.
        agilityContext.clear(metaSubspace.pack(name));
        return true;
    }

    public boolean usesOptimizedStoredFields() {
        return getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, false)
                || getBooleanIndexOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, false);
    }

    /**
     * Returns the size of the given file under the given name.
     * @param name the name of the file reference
     * @return long value of the size of the file
     * @throws NoSuchFileException if the file reference doesn't exist.
     */
    @Override
    public long fileLength(@Nonnull String name) throws NoSuchFileException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("fileLength",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        long startTime = System.nanoTime();
        try {
            FDBLuceneFileReference reference = asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LENGTH, getFDBLuceneFileReferenceAsync(name));
            if (reference == null) {
                throw new NoSuchFileException(name);
            }
            return reference.getSize();
        } finally {
            agilityContext.recordEvent(LuceneEvents.Events.LUCENE_GET_FILE_LENGTH, System.nanoTime() - startTime);
        }
    }

    /**
     * Create new output for a file.
     * @param name the filename to create
     * @param ioContext the IOContext from Lucene
     * @return IndexOutput FDB Backed Index Output FDBIndexOutput
     */
    @Override
    @Nonnull
    @SuppressWarnings("java:S2093")
    public IndexOutput createOutput(@Nonnull final String name, @Nullable final IOContext ioContext) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("createOutput",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        long startTime = System.nanoTime();
        try {
            // Unlike other segment files the .si, .cfe, .fnm are not added to the .cfs when the compound file is created.
            // A rollback could cause the .cfs to be deleted, but leave the other files around.
            // But, these files are small, and are always read, so instead of storing them in the same way as other files,
            // we store them as bytes on the FileReference itself.
            // This approach means that we have a single range-read to do listAll, but won't have to additional point-reads
            // for these files.
            if (FDBDirectory.isSegmentInfo(name) || FDBDirectory.isEntriesFile(name)) {
                long id = getIncrement();
                return new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), name, name, new CRC32(), dataOutput -> {
                    final byte[] content = dataOutput.toArrayCopy();
                    writeFDBLuceneFileReference(name, new FDBLuceneFileReference(id, content));
                });
            } else if (FDBDirectory.isFieldInfoFile(name)) {
                return new EmptyIndexOutput(name, name, this);
            } else {
                return new FDBIndexOutput(name, name, this);
            }
        } finally {
            agilityContext.recordEvent(LuceneEvents.Waits.WAIT_LUCENE_CREATE_OUTPUT, System.nanoTime() - startTime);
        }
    }

    /**
     * Lucene uses temporary files internally that are actually serialized onto disk.  Once on disk,
     * the files can be renamed.
     *
     * @param prefix prefix
     * @param suffix suffix
     * @param ioContext ioContext
     * @return IndexOutput
     */
    @Override
    @Nonnull
    public IndexOutput createTempOutput(@Nonnull final String prefix, @Nonnull final String suffix, @Nonnull final IOContext ioContext) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("createTempOutput",
                    LuceneLogMessageKeys.FILE_PREFIX, prefix,
                    LuceneLogMessageKeys.FILE_SUFFIX, suffix));
        }
        return createOutput(getTempFileName(prefix, suffix, this.nextTempFileCounter.getAndIncrement()), ioContext);
    }

    @Nonnull
    protected static String getTempFileName(@Nonnull String prefix, @Nonnull String suffix, long counter) {
        return IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(counter, 36), "tmp");
    }

    @Override
    public void sync(@Nonnull final Collection<String> collection) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("sync",
                    LuceneLogMessageKeys.FILE_NAME, String.join(", ", collection)));
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("syncMetaData"));
        }
    }

    /**
     * It is the caller's responsibility to make the dest (destination) does not exist.
     *
     * @param source source
     * @param dest desc
     */
    @Override
    public void rename(@Nonnull final String source, @Nonnull final String dest) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("rename",
                    LogMessageKeys.SOURCE_FILE, source,
                    LuceneLogMessageKeys.DEST_FILE, dest));
        }
        try {
            final byte[] key = metaSubspace.pack(source);
            asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_RENAME, getFileReferenceCacheAsync().thenApply(cache -> {
                final FDBLuceneFileReference value = cache.get(source);
                if (value == null) {
                    throw new RecordCoreArgumentException("Invalid source name in rename function for source")
                            .addLogInfo(LogMessageKeys.SOURCE_FILE, source)
                            .addLogInfo(LogMessageKeys.INDEX_TYPE, LuceneIndexTypes.LUCENE)
                            .addLogInfo(LogMessageKeys.SUBSPACE, subspace)
                            .addLogInfo(LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compressionEnabled)
                            .addLogInfo(LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encryptionEnabled);
                }
                byte[] encodedBytes = LuceneSerializer.encode(value.getBytes(), compressionEnabled, encryptionEnabled);
                agilityContext.set(metaSubspace.pack(dest), encodedBytes);
                agilityContext.clear(key);

                cache.remove(source);
                cache.put(dest, value);

                return null;
            }));
        } finally {
            agilityContext.increment(LuceneEvents.Counts.LUCENE_RENAME_FILE);
        }
    }

    @Override
    @Nonnull
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("openInput",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        if (FDBDirectory.isSegmentInfo(name) || FDBDirectory.isEntriesFile(name)) {
            final FDBLuceneFileReference reference = getFDBLuceneFileReference(name);
            if (reference.getContent().isEmpty()) {
                throw new RecordCoreException("File content is not stored in reference")
                        .addLogInfo(LuceneLogMessageKeys.FILE_NAME, name);
            } else {
                return new ByteBuffersIndexInput(
                        new ByteBuffersDataInput(reference.getContent().asReadOnlyByteBufferList()), name);
            }
        } else if (FDBDirectory.isFieldInfoFile(name) || FDBDirectory.isStoredFieldsFile(name)) {
            return new EmptyIndexInput(name);
        } else {
            // the contract is that this should throw an exception, but we don't
            // https://github.com/FoundationDB/fdb-record-layer/issues/2361
            return new FDBIndexInput(name, this);
        }
    }

    @Override
    @Nonnull
    public Lock obtainLock(@Nonnull final String lockName) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("obtainLock",
                    LuceneLogMessageKeys.LOCK_NAME, lockName));
        }
        final Lock lock = lockFactory.obtainLock(null, lockName);
        lastLock = (FDBDirectoryLockFactory.FDBDirectoryLock) lock;
        return lock;
    }

    private void clearLockIfLocked() {
        if (lastLock != null) {
            lastLock.fileLockClearIfLocked();
        }
    }

    /**
     * No Op that reports in debug mode the block and file reference cache stats.
     */
    @Override
    public void close() {
        try {
            clearLockIfLocked();     // no-op if already closed. This call may or may not be called in a recovery path.
            agilityContext.flush();  // no-op if already flushed or closed.
        } catch (RuntimeException ex) {
            // Here: got exception, it is important to clear the file lock, or it will prevent retry-recovery
            agilityContext.abortAndClose();
            clearLockIfLocked();     // after closing, this call will be performed in a recovery path
            throw ex;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(fileListLog("Closed FDBDirectory", Objects.requireNonNullElse(fileReferenceCache.get(), Map.of()))
                    .addKeyAndValue(LuceneLogMessageKeys.BLOCK_CACHE_STATS, blockCache.stats())
                    .toString());
        }
    }

    /**
     * We delete inline vs. batching them together.
     *
     * @return Emtpy set of strings
     */
    @Override
    @Nonnull
    public Set<String> getPendingDeletions() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("getPendingDeletions"));
        }
        return Collections.emptySet();
    }

    public int getBlockSize() {
        return blockSize;
    }

    @VisibleForTesting
    FDBRecordContext getCallerContext() {
        // We cannot control the way the returned context is used. Limiting it to testing seems to be a good idea.
        return agilityContext.getCallerContext();
    }

    public AgilityContext getAgilityContext() {
        return agilityContext;
    }

    @Nullable
    public <T> T asyncToSync(StoreTimer.Wait event,
                             @Nonnull CompletableFuture<T> async ) {
        return agilityContext.asyncToSync(event, async);
    }

    public Subspace getSubspace() {
        return subspace;
    }

    @Nonnull
    private String getLogMessage(@Nonnull String staticMsg, @Nullable final Object... keysAndValues) {
        return getKeyValueLogMessage(staticMsg, keysAndValues).toString();
    }

    private KeyValueLogMessage getKeyValueLogMessage(final @Nonnull String staticMsg, final Object... keysAndValues) {
        return KeyValueLogMessage.build(staticMsg, keysAndValues)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, subspace)
                .addKeyAndValue(LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compressionEnabled)
                .addKeyAndValue(LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encryptionEnabled);
    }

    /**
     * Places a prefetchable buffer over the checksum input in an attempt to pipeline reads when an
     * FDBIndexOutput performs a copyBytes operation.
     *
     * @param name file name
     * @param context io context
     * @return ChecksumIndexInput
     * @throws IOException ioexception
     */
    @Override
    public ChecksumIndexInput openChecksumInput(final String name, final IOContext context) throws IOException {
        return new PrefetchableBufferedChecksumIndexInput(openInput(name, context));
    }

    Cache<ComparablePair<Long, Integer>, CompletableFuture<byte[]>> getBlockCache() {
        return blockCache;
    }

    /**
     * Get a primary key segment index if enabled.
     * @return index or {@code null} if not enabled
     */
    @Nullable
    public LucenePrimaryKeySegmentIndex getPrimaryKeySegmentIndex() {
        if (getBooleanIndexOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, false)) {
            synchronized (this) {
                if (primaryKeySegmentIndex == null) {
                    final Subspace primaryKeySubspace = subspace.subspace(Tuple.from(PRIMARY_KEY_SUBSPACE));
                    primaryKeySegmentIndex = new LucenePrimaryKeySegmentIndexV1(this, primaryKeySubspace);
                }
            }
            return primaryKeySegmentIndex;
        } else if (getBooleanIndexOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, false)) {
            synchronized (this) {
                if (primaryKeySegmentIndex == null) {
                    final Subspace primaryKeySubspace = subspace.subspace(Tuple.from(PRIMARY_KEY_SUBSPACE));
                    primaryKeySegmentIndex = new LucenePrimaryKeySegmentIndexV2(this, primaryKeySubspace);
                }
            }
            return primaryKeySegmentIndex;
        } else {
            return null;
        }
    }

    // Map segment name to integer id.
    // TODO: Could store this elsewhere, such as inside compound file.
    public long primaryKeySegmentId(@Nonnull String segmentName, boolean create) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segmentName, "", "pky");
        FDBLuceneFileReference ref = getFDBLuceneFileReference(fileName);
        if (ref == null) {
            if (!create) {
                throw new NoSuchFileException(segmentName);
            }
            ref = new FDBLuceneFileReference(getIncrement(), 0, 0, 0);
            writeFDBLuceneFileReference(fileName, ref);
        }
        return ref.getId();
    }

    byte[] fileLockKey(String lockName) {
        return fileLockSubspace.pack(Tuple.from(lockName));
    }


    // Map stored segment id back to segment name.
    @Nullable
    public String primaryKeySegmentName(long segmentId) {
        for (Map.Entry<String, FDBLuceneFileReference> entry : getFileReferenceCache().entrySet()) {
            if (entry.getValue().getId() == segmentId) {
                final String fileName = entry.getKey();
                if (!fileName.endsWith(".pky")) {
                    throw new IllegalArgumentException("Given segment id is not for a pky file: " + fileName);
                }
                return fileName.substring(0, fileName.length() - 4);
            }
        }
        return null;
    }

    /**
     * Convenience methods to get index options from the directory's index.
     * @param key the option key
     * @param defaultValue the value to use when the option is not set
     * @return the index option value, or the default value if not found
     */
    public boolean getBooleanIndexOption(@Nonnull String key, boolean defaultValue) {
        final String option = getIndexOption(key);
        if (option == null) {
            return defaultValue;
        } else {
            return Boolean.valueOf(option);
        }
    }

    /**
     * Convenience methods to get index options from the directory's index.
     * @param key the option key
     * @return the index option value, null if not found
     */
    @Nullable
    public String getIndexOption(@Nonnull String key) {
        return indexOptions.get(key);
    }
}

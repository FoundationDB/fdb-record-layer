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
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.codec.PrefetchableBufferedChecksumIndexInput;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
public class FDBDirectory extends Directory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectory.class);
    public static final int DEFAULT_BLOCK_SIZE = 1_024;
    public static final int DEFAULT_MAXIMUM_SIZE = 1024;
    public static final int DEFAULT_CONCURRENCY_LEVEL = 16;
    public static final int DEFAULT_INITIAL_CAPACITY = 128;
    private static final int SEQUENCE_SUBSPACE = 0;
    private static final int META_SUBSPACE = 1;
    private static final int DATA_SUBSPACE = 2;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final FDBRecordContext context;
    private final Subspace subspace;
    private final Subspace metaSubspace;
    private final Subspace dataSubspace;
    private final byte[] sequenceSubspaceKey;

    private final LockFactory lockFactory;
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

    private final AtomicLong fileSequenceCounter;
    private final Cache<Pair<Long, Integer>, CompletableFuture<byte[]>> blockCache;

    private final boolean compressionEnabled;
    private final boolean encryptionEnabled;

    // The shared cache is initialized when first listing the directory, if a manager is present, and cleared before writing.
    @Nullable
    private final FDBDirectorySharedCacheManager sharedCacheManager;
    @Nullable
    private final Tuple sharedCacheKey;
    @Nullable
    private FDBDirectorySharedCache sharedCache;
    // True if sharedCacheManager is present until sharedCache has been set (or not).
    private boolean sharedCachePending;

    public FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context) {
        this(subspace, context, null, null);
    }

    public FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context,
                        @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey) {
        this(subspace, context, sharedCacheManager, sharedCacheKey, NoLockFactory.INSTANCE);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context,
                 @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey,
                 @Nonnull LockFactory lockFactory) {
        this(subspace, context, sharedCacheManager, sharedCacheKey, lockFactory,
                DEFAULT_BLOCK_SIZE, DEFAULT_INITIAL_CAPACITY, DEFAULT_MAXIMUM_SIZE, DEFAULT_CONCURRENCY_LEVEL);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context,
                 @Nullable FDBDirectorySharedCacheManager sharedCacheManager, @Nullable Tuple sharedCacheKey,
                 @Nonnull LockFactory lockFactory,
                 int blockSize, final int initialCapacity, final int maximumSize, final int concurrencyLevel) {
        Verify.verify(subspace != null);
        Verify.verify(context != null);
        Verify.verify(lockFactory != null);
        this.context = context;
        this.subspace = subspace;
        final Subspace sequenceSubspace = subspace.subspace(Tuple.from(SEQUENCE_SUBSPACE));
        this.sequenceSubspaceKey = sequenceSubspace.pack();
        this.metaSubspace = subspace.subspace(Tuple.from(META_SUBSPACE));
        this.dataSubspace = subspace.subspace(Tuple.from(DATA_SUBSPACE));
        this.lockFactory = lockFactory;
        this.blockSize = blockSize;
        this.fileReferenceCache = new AtomicReference<>();
        this.blockCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maximumSize)
                .recordStats()
                .build();
        this.fileSequenceCounter = new AtomicLong(-1);
        this.compressionEnabled = Objects.requireNonNullElse(context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED), false);
        this.encryptionEnabled = Objects.requireNonNullElse(context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_ENCRYPTION_ENABLED), false);
        this.fileReferenceMapSupplier = Suppliers.memoize(this::loadFileReferenceCacheForMemoization);
        this.sharedCacheManager = sharedCacheManager;
        this.sharedCacheKey = sharedCacheKey;
        this.sharedCachePending = sharedCacheManager != null && sharedCacheKey != null;
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
        return context.ensureActive().get(sequenceSubspaceKey).thenAccept(serializedValue -> {
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

        context.increment(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS);

        // Make sure the file counter is loaded from the database into the fileSequenceCounter, then
        // use the cached value
        context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_INCREMENT, loadFileSequenceCounter());
        long incrementedValue = fileSequenceCounter.incrementAndGet();

        // Use BYTE_MAX here so that if there are concurrent calls (which will get different increments because they
        // are serialized around the fileSequenceCounter AtomicLong), the largest one always gets
        // committed into the database
        byte[] serializedValue = serializeFileSequenceCounter(incrementedValue);
        context.ensureActive().mutate(MutationType.BYTE_MAX, sequenceSubspaceKey, serializedValue);

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
        return context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE, getFDBLuceneFileReferenceAsync(name));
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

    public static String convertToDataFile(String name) {
        if (isSegmentInfo(name)) {
            return name.substring(0, name.length() - 2) + DATA_EXTENSION;
        } else if (isEntriesFile(name)) {
            return name.substring(0, name.length() - 3) + DATA_EXTENSION;
        } else {
            return name;
        }
    }

    /**
     * Puts a file reference in the meta subspace and in the cache under the given name.
     * @param name name for the file reference
     * @param reference the file reference being inserted
     */
    public void writeFDBLuceneFileReference(@Nonnull String name, @Nonnull FDBLuceneFileReference reference) {
        if (isEntriesFile(name)) {
            name = convertToDataFile(name);
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name);
            if (storedRef != null) {
                storedRef.setEntries(reference.getEntries());
                reference = storedRef;
            }
        } else if (isSegmentInfo(name)) {
            name = convertToDataFile(name);
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name);
            if (storedRef != null) {
                storedRef.setSegmentInfo(reference.getSegmentInfo());
                reference = storedRef;
            }
        } else if (isCompoundFile(name)) {
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name);
            if (storedRef != null) {
                reference.setSegmentInfo(storedRef.getSegmentInfo());
                reference.setEntries(storedRef.getEntries());
            }
        }
        final byte[] fileReferenceBytes = reference.getBytes();
        final byte[] encodedBytes = Objects.requireNonNull(LuceneSerializer.encode(reference.getBytes(), compressionEnabled, encryptionEnabled));
        context.increment(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_SIZE, encodedBytes.length);
        context.increment(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_CALL);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene file reference",
                    LuceneLogMessageKeys.FILE_NAME, name,
                    LuceneLogMessageKeys.DATA_SIZE, fileReferenceBytes.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encodedBytes.length,
                    LuceneLogMessageKeys.FILE_REFERENCE, reference));
        }
        context.ensureActive().set(metaSubspace.pack(name), encodedBytes);
        getFileReferenceCache().put(name, reference);
    }

    /**
     * Writes data to the given block under the given id.
     * @param id id for the data
     * @param block block for the data to be stored in
     * @param value the data to be stored
     * @return the actual data size written to database with potential compression and encryption applied
     */
    public CompletableFuture<Integer> writeData(final long id, final int block, @Nonnull final byte[] value) {
        return CompletableFuture.supplyAsync( () -> {
            final byte[] encodedBytes = Objects.requireNonNull(LuceneSerializer.encode(value, compressionEnabled, encryptionEnabled));
            //This may not be correct transactionally
            context.increment(LuceneEvents.Counts.LUCENE_WRITE_SIZE, encodedBytes.length);
            context.increment(LuceneEvents.Counts.LUCENE_WRITE_CALL);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(getLogMessage("Write lucene data",
                        LuceneLogMessageKeys.FILE_ID, id,
                        LuceneLogMessageKeys.BLOCK_NUMBER, block,
                        LuceneLogMessageKeys.DATA_SIZE, value.length,
                        LuceneLogMessageKeys.ENCODED_DATA_SIZE, encodedBytes.length));
            }
            Verify.verify(value.length <= blockSize);
            context.ensureActive().set(dataSubspace.pack(Tuple.from(id, block)), encodedBytes);
            return encodedBytes.length;
        });
    }

    /**
     * Reads known data from the directory.
     * @param resourceDescription Description should be non-null, opaque string describing this resource; used for logging
     * @param referenceFuture the reference where the data supposedly lives
     * @param block the block where the data is stored
     * @return Completable future of the data returned
     * @throws RecordCoreException if blockCache fails to get the data from the block
     * @throws RecordCoreArgumentException if a reference with that id hasn't been written yet.
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public CompletableFuture<byte[]> readBlock(@Nonnull String resourceDescription, @Nonnull CompletableFuture<FDBLuceneFileReference> referenceFuture, int block) {
        return referenceFuture.thenCompose(reference -> readBlock(resourceDescription, reference, block));
    }

    @Nonnull
    private CompletableFuture<byte[]> readBlock(@Nonnull String resourceDescription, @Nullable FDBLuceneFileReference reference, int block) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("readBlock",
                    LuceneLogMessageKeys.FILE_NAME, resourceDescription,
                    LuceneLogMessageKeys.BLOCK_NUMBER, block));
        }
        if (reference == null) {
            CompletableFuture<byte[]> exceptionalFuture = new CompletableFuture<>();
            exceptionalFuture.completeExceptionally(new RecordCoreArgumentException(String.format("No reference with name %s was found", resourceDescription)));
            return exceptionalFuture;
        }
        final long id = reference.getId();
        return context.instrument(LuceneEvents.Events.LUCENE_READ_BLOCK, blockCache.asMap().computeIfAbsent(Pair.of(id, block), ignore -> {
                    if (sharedCache == null) {
                        return readData(id, block);
                    }
                    final byte[] fromShared = sharedCache.getBlockIfPresent(id, block);
                    if (fromShared != null) {
                        context.increment(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS);
                        return CompletableFuture.completedFuture(fromShared);
                    } else {
                        context.increment(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES);
                        return readData(id, block).thenApply(data -> {
                            sharedCache.putBlockIfAbsent(id, block, data);
                            return data;
                        });
                    }
                }
        ));
    }

    private CompletableFuture<byte[]> readData(long id, int block) {
        return context.instrument(LuceneEvents.Events.LUCENE_FDB_READ_BLOCK,
                context.ensureActive().get(dataSubspace.pack(Tuple.from(id, block)))
                        .thenApply(LuceneSerializer::decode));
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
            Set<String> outSet = getFileReferenceCache().keySet();
            return outSet.toArray(new String[0]);
        } finally {
            context.record(LuceneEvents.Events.LUCENE_LIST_ALL, System.nanoTime() - startTime);
        }
    }

    private CompletableFuture<Void> loadFileReferenceCacheForMemoization() {
        long start = System.nanoTime();
        final ConcurrentSkipListMap<String, FDBLuceneFileReference> outMap = new ConcurrentSkipListMap<>();
        // Issue a range read with StreamingMode.WANT_ALL (instead of default ITERATOR) because this needs to read the
        // entire range before doing anything with the data
        final AsyncIterable<KeyValue> rangeIterable = context.ensureActive()
                .getRange(metaSubspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL);
        CompletableFuture<Void> future = AsyncUtil.forEach(rangeIterable, kv -> {
            String name = metaSubspace.unpack(kv.getKey()).getString(0);
            final FDBLuceneFileReference fileReference = Objects.requireNonNull(FDBLuceneFileReference.parseFromBytes(LuceneSerializer.decode(kv.getValue())));
            outMap.put(name, fileReference);
        }, context.getExecutor()).thenAccept(ignore -> {
            if (LOGGER.isDebugEnabled()) {
                List<String> displayList = new ArrayList<>(outMap.size());
                long totalSize = 0L;
                long actualTotalSize = 0L;
                for (Map.Entry<String, FDBLuceneFileReference> entry: outMap.entrySet()) {
                    displayList.add(entry.getKey());
                    totalSize += entry.getValue().getSize();
                    actualTotalSize += entry.getValue().getActualSize();
                }
                LOGGER.debug(getLogMessage("listAllFiles",
                        LuceneLogMessageKeys.FILE_COUNT, displayList.size(),
                        LuceneLogMessageKeys.FILE_LIST, displayList,
                        LuceneLogMessageKeys.FILE_TOTAL_SIZE, totalSize,
                        LuceneLogMessageKeys.FILE_ACTUAL_TOTAL_SIZE, actualTotalSize));
            }

            // Memoize the result in an FDBDirectory member variable. Future attempts to access files
            // should use the class variable
            fileReferenceCache.compareAndSet(null, outMap);
        });
        return context.instrument(LuceneEvents.Events.LUCENE_LOAD_FILE_CACHE, future, start);
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
                    sharedCachePending = false;
                    return CompletableFuture.completedFuture(fromShared);
                }
                return fileReferenceMapSupplier.get().thenApply(ignore -> {
                    final ConcurrentSkipListMap<String, FDBLuceneFileReference> fromSupplier = fileReferenceCache.get();
                    sharedCache.setFileReferencesIfAbsent(fromSupplier);
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
        return Objects.requireNonNull(context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_LOAD_FILE_CACHE, getFileReferenceCacheAsync()));
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

        if (isEntriesFile(name) || isSegmentInfo(name)) {
            return;
        }
        boolean deleted = Objects.requireNonNull(context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE, getFileReferenceCacheAsync().thenApply(cache -> {
            FDBLuceneFileReference value = cache.get(name);
            if (value == null) {
                return false;
            }
            context.ensureActive().clear(metaSubspace.pack(name));
            context.ensureActive().clear(dataSubspace.subspace(Tuple.from(value.getId())).range());
            cache.remove(name);
            return true;
        })));

        if (!deleted) {
            throw new NoSuchFileException(name);
        }
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
            name = convertToDataFile(name);
            FDBLuceneFileReference reference = context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LENGTH, getFDBLuceneFileReferenceAsync(name));
            if (reference == null) {
                throw new NoSuchFileException(name);
            }
            if (isEntriesFile(name)) {
                return reference.getEntries().length;
            }
            if (isSegmentInfo(name)) {
                return reference.getSegmentInfo().length;
            }
            return reference.getSize();
        } finally {
            context.record(LuceneEvents.Events.LUCENE_GET_FILE_LENGTH, System.nanoTime() - startTime);
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
    public IndexOutput createOutput(@Nonnull final String name, @Nullable final IOContext ioContext) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("createOutput",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        return new FDBIndexOutput(name, name, this);
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
    public IndexOutput createTempOutput(@Nonnull final String prefix, @Nonnull final String suffix, @Nonnull final IOContext ioContext) {
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
        final byte[] key = metaSubspace.pack(source);
        context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_RENAME, getFileReferenceCacheAsync().thenApply(cache -> {
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
            context.ensureActive().set(metaSubspace.pack(dest), encodedBytes);
            context.ensureActive().clear(key);

            cache.remove(source);
            cache.put(dest, value);

            return null;
        }));
    }

    @Override
    @Nonnull
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("openInput",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        return new FDBIndexInput(name, this);
    }

    public IndexInput openLazyInput(@Nonnull final String name, @Nonnull final IOContext ioContext, long initialOffset, long position) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("openInput",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        return new FDBIndexInput(name, this, initialOffset, position);
    }

    @Override
    @Nonnull
    public Lock obtainLock(@Nonnull final String lockName) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("obtainLock",
                    LuceneLogMessageKeys.LOCK_NAME, lockName));
        }
        return lockFactory.obtainLock(null, lockName);
    }

    /**
     * No Op that reports in debug mode the block and file reference cache stats.
     */
    @Override
    public void close() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("close called",
                    LuceneLogMessageKeys.BLOCK_CACHE_STATS, blockCache.stats()));
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

    public FDBRecordContext getContext() {
        return context;
    }

    public Subspace getSubspace() {
        return subspace;
    }

    @Nonnull
    private String getLogMessage(@Nonnull String staticMsg, @Nullable final Object... keysAndValues) {
        return KeyValueLogMessage.build(staticMsg, keysAndValues)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, subspace)
                .addKeyAndValue(LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compressionEnabled)
                .addKeyAndValue(LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encryptionEnabled)
                .toString();
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

    Cache<Pair<Long, Integer>, CompletableFuture<byte[]>> getBlockCache() {
        return blockCache;
    }

}

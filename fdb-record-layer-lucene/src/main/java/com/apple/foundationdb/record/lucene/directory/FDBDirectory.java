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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexFileNames;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
    public static final int DEFAULT_BLOCK_SIZE = 16_384;
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
    private final Cache<String, FDBLuceneFileReference> fileReferenceCache;
    private final Cache<Pair<Long, Integer>, CompletableFuture<byte[]>> blockCache;

    private final boolean compressionEnabled;
    private final boolean encryptionEnabled;

    public FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context) {
        this(subspace, context, NoLockFactory.INSTANCE);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull LockFactory lockFactory) {
        this(subspace, context, lockFactory, DEFAULT_BLOCK_SIZE, DEFAULT_INITIAL_CAPACITY, DEFAULT_MAXIMUM_SIZE, DEFAULT_CONCURRENCY_LEVEL);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull LockFactory lockFactory,
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
        this.fileReferenceCache = CacheBuilder.newBuilder()
                .initialCapacity(initialCapacity)
                .maximumSize(maximumSize)
                .recordStats()
                .build();
        this.blockCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maximumSize)
                .recordStats()
                .build();
        this.compressionEnabled = Objects.requireNonNullElse(context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED), false);
        this.encryptionEnabled = Objects.requireNonNullElse(context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_ENCRYPTION_ENABLED), false);
    }

    /**
     * Sets increment if not set yet and waits till completed to return
     * Returns and increments the increment if its already set.
     * @return current increment value
     */
    public synchronized long getIncrement() {
        increment(LuceneEvents .Counts.LUCENE_GET_INCREMENT_CALLS);
        return context.ensureActive().get(sequenceSubspaceKey).thenApply(
            (value) -> {
                if (value == null) {
                    context.ensureActive().set(sequenceSubspaceKey, Tuple.from(1L).pack());
                    return 1L;
                } else {
                    long sequence = Tuple.fromBytes(value).getLong(0) + 1;
                    context.ensureActive().set(sequenceSubspaceKey, Tuple.from(sequence).pack());
                    return sequence;
                }
            }).join();
    }

    private void increment(StoreTimer.Count counter) {
        final FDBStoreTimer timer = context.getTimer();
        if (timer != null) {
            timer.increment(counter);
        }
    }

    private void increment(StoreTimer.Count counter, int amount) {
        final FDBStoreTimer timer = context.getTimer();
        if (timer != null) {
            timer.increment(counter, amount);
        }
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
    @Nonnull
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReference(@Nonnull final String name) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("getFDBLuceneFileReference",
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        FDBLuceneFileReference fileReference = this.fileReferenceCache.getIfPresent(name);
        if (fileReference == null) {
            return context.instrument(LuceneEvents.Events.LUCENE_GET_FILE_REFERENCE, context.ensureActive().get(metaSubspace.pack(name))
                    .thenApply((value) -> {
                        final FDBLuceneFileReference fetchedRef = FDBLuceneFileReference.parseFromBytes(LuceneSerializer.decode(value));
                        if (fetchedRef != null) {
                            this.fileReferenceCache.put(name, fetchedRef);
                        }
                        return fetchedRef;
                    }), System.nanoTime());
        } else {
            return CompletableFuture.completedFuture(fileReference);
        }
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
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name).join();
            if (storedRef != null) {
                storedRef.setEntries(reference.getEntries());
                reference = storedRef;
            }
        } else if (isSegmentInfo(name)) {
            name = convertToDataFile(name);
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name).join();
            if (storedRef != null) {
                storedRef.setSegmentInfo(reference.getSegmentInfo());
                reference = storedRef;
            }
        } else if (isCompoundFile(name)) {
            FDBLuceneFileReference storedRef = getFDBLuceneFileReference(name).join();
            if (storedRef != null) {
                reference.setSegmentInfo(storedRef.getSegmentInfo());
                reference.setEntries(storedRef.getEntries());
            }
        }
        final byte[] fileReferenceBytes = reference.getBytes();
        final byte[] encodedBytes = LuceneSerializer.encode(reference.getBytes(), compressionEnabled, encryptionEnabled);
        increment(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_SIZE, encodedBytes.length);
        increment(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_CALL);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("Write lucene file reference",
                    LuceneLogMessageKeys.FILE_NAME, name,
                    LuceneLogMessageKeys.DATA_SIZE, fileReferenceBytes.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encodedBytes.length,
                    LuceneLogMessageKeys.FILE_REFERENCE, reference));
        }
        context.ensureActive().set(metaSubspace.pack(name), encodedBytes);
        fileReferenceCache.put(name, reference);
    }

    /**
     * Writes data to the given block under the given id.
     * @param id id for the data
     * @param block block for the data to be stored in
     * @param value the data to be stored
     * @return the actual data size written to database with potential compression and encryption applied
     */
    public int writeData(long id, int block, @Nonnull byte[] value) {
        final byte[] encodedBytes = LuceneSerializer.encode(value, compressionEnabled, encryptionEnabled);
        //This may not be correct transactionally
        increment(LuceneEvents.Counts.LUCENE_WRITE_SIZE, encodedBytes.length);
        increment(LuceneEvents.Counts.LUCENE_WRITE_CALL);
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
    @SuppressWarnings("PMD.UnusedNullCheckInEquals") // checks and throws more relevant exception
    @Nonnull
    public CompletableFuture<byte[]> readBlock(@Nonnull String resourceDescription, @Nonnull CompletableFuture<FDBLuceneFileReference> referenceFuture, int block) throws RecordCoreException {
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(getLogMessage("readBlock",
                        LuceneLogMessageKeys.FILE_NAME, resourceDescription,
                        LuceneLogMessageKeys.BLOCK_NUMBER, block));
            }
            final FDBLuceneFileReference reference = referenceFuture.join(); // Tried to fully pipeline this but the reality is that this is mostly cached after listAll, delete, etc.
            if (reference == null) {
                throw new RecordCoreArgumentException(String.format("No reference with name %s was found", resourceDescription));
            }
            Long id = reference.getId();

            long start = System.nanoTime();
            return context.instrument(LuceneEvents.Events.LUCENE_READ_BLOCK,blockCache.get(Pair.of(id, block),
                    () -> context.instrument(LuceneEvents.Events.LUCENE_FDB_READ_BLOCK,
                            context.ensureActive().get(dataSubspace.pack(Tuple.from(id, block)))
                                    .thenApplyAsync(data -> LuceneSerializer.decode(data)))
            ), start);
        } catch (ExecutionException e) {
            throw new RecordCoreException(CompletionExceptionLogHelper.asCause(e));
        }
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
        List<String> outList;
        long start = System.nanoTime();
        try {
            outList = listAllInternal();
        } finally {
            record(LuceneEvents.Events.LUCENE_LIST_ALL, System.nanoTime() - start);
        }
        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return outList.toArray(new String[outList.size()]);
    }

    private void record(StoreTimer.Event event, long durationNanos) {
        final FDBStoreTimer timer = context.getTimer();
        if (timer != null) {
            timer.record(event, durationNanos);
        }
    }

    private List<String> listAllInternal() {
        //A private form of the method to allow easier instrumentation
        List<String> outList = new ArrayList<>();
        List<String> displayList = null;

        long totalSize = 0L;
        long actualTotalSize = 0L;
        for (KeyValue kv : context.ensureActive().getRange(metaSubspace.range())) {
            String name = metaSubspace.unpack(kv.getKey()).getString(0);
            outList.add(name);
            final FDBLuceneFileReference fileReference = FDBLuceneFileReference.parseFromBytes(LuceneSerializer.decode(kv.getValue()));
            // Only composite files are prefetched.
            if (name.endsWith(".cfs")) {
                try {
                    readBlock(name, CompletableFuture.completedFuture(fileReference), 0);
                } catch (RecordCoreException e) {
                    LOGGER.warn(getLogMessage("Exception thrown during prefetch",
                            LuceneLogMessageKeys.FILE_NAME, name));
                }
            }
            this.fileReferenceCache.put(name, fileReference);
            if (LOGGER.isDebugEnabled()) {
                if (displayList == null) {
                    displayList = new ArrayList<>();
                }
                if (kv.getValue() != null) {
                    displayList.add(name + "(" + fileReference.getSize() + ")");
                    totalSize += fileReference.getSize();
                    actualTotalSize += fileReference.getActualSize();
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getLogMessage("listAllFiles",
                    LuceneLogMessageKeys.FILE_COUNT, outList.size(),
                    LuceneLogMessageKeys.FILE_LIST, displayList,
                    LuceneLogMessageKeys.FILE_TOTAL_SIZE, totalSize,
                    LuceneLogMessageKeys.FILE_ACTUAL_TOTAL_SIZE, actualTotalSize));
        }
        return outList;
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
        boolean deleted = context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE, getFDBLuceneFileReference(name).thenApplyAsync(
                (value) -> {
                    if (value == null) {
                        return false;
                    }
                    context.ensureActive().clear(metaSubspace.pack(name));
                    context.ensureActive().clear(dataSubspace.subspace(Tuple.from(value.getId())).range());
                    this.fileReferenceCache.invalidate(name);
                    return true;
                }
        ));

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
        name = convertToDataFile(name);
        FDBLuceneFileReference reference = context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LENGTH, getFDBLuceneFileReference(name));
        if (isEntriesFile(name)) {
            return reference.getEntries().length;
        }
        if (isSegmentInfo(name)) {
            return reference.getSegmentInfo().length;
        }
        if (reference == null) {
            throw new NoSuchFileException(name);
        }
        return reference.getSize();
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
        context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_RENAME, context.ensureActive().get(key).thenApply((Function<byte[], Void>)value -> {
            if (value == null) {
                throw new RecordCoreArgumentException("Invalid source name in rename function for source")
                        .addLogInfo(LogMessageKeys.SOURCE_FILE,source)
                        .addLogInfo(LogMessageKeys.INDEX_TYPE, LuceneIndexTypes.LUCENE)
                        .addLogInfo(LogMessageKeys.SUBSPACE, subspace)
                        .addLogInfo(LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compressionEnabled)
                        .addLogInfo(LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encryptionEnabled);
            }
            fileReferenceCache.invalidate(source);
            context.ensureActive().set(metaSubspace.pack(dest), value);
            context.ensureActive().clear(key);

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
                    LuceneLogMessageKeys.BLOCK_CACHE_STATS, blockCache.stats(),
                    LuceneLogMessageKeys.REFERENCE_CACHE_STATUS, fileReferenceCache.stats()));
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
}

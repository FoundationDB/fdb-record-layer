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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final Transaction txn;
    private final Subspace subspace;
    private final Subspace metaSubspace;
    private final Subspace dataSubspace;
    private final byte[] sequenceSubspaceKey;

    private final LockFactory lockFactory;
    private final int blockSize;
    private final Cache<String, FDBLuceneFileReference> fileReferenceCache;
    private final Cache<Pair<Long, Integer>, CompletableFuture<byte[]>> blockCache;
    private final Map<String, Long> reads;

    public FDBDirectory(@Nonnull Subspace subspace, @Nonnull Transaction txn) {
        this(subspace, txn, NoLockFactory.INSTANCE);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull Transaction txn, @Nonnull LockFactory lockFactory) {
        this(subspace, txn, lockFactory, DEFAULT_BLOCK_SIZE);
    }

    FDBDirectory(@Nonnull Subspace subspace, @Nonnull Transaction txn, @Nonnull LockFactory lockFactory, int blockSize) {
        Verify.verify(subspace != null);
        Verify.verify(txn != null);
        Verify.verify(lockFactory != null);
        this.txn = txn;
        this.subspace = subspace;
        final Subspace sequenceSubspace = subspace.subspace(Tuple.from(0));
        this.sequenceSubspaceKey = sequenceSubspace.pack();
        this.metaSubspace = subspace.subspace(Tuple.from(1));
        this.dataSubspace = subspace.subspace(Tuple.from(2));
        this.lockFactory = lockFactory;
        this.blockSize = blockSize;
        this.fileReferenceCache = CacheBuilder.newBuilder().initialCapacity(128).maximumSize(1024).recordStats().build();
        this.blockCache = CacheBuilder.newBuilder().concurrencyLevel(16).initialCapacity(128).maximumSize(1024).recordStats().build();
        this.reads = new ConcurrentHashMap<>();
    }

    /**
     * Sets increment if not set yet and waits till completed to return
     * Returns and increments the increment if its already set.
     * @return current increment value
     */
    public synchronized long getIncrement() {
        return txn.get(sequenceSubspaceKey).thenApply(
            (value) -> {
                if (value == null) {
                    txn.set(sequenceSubspaceKey, Tuple.from(1L).pack());
                    return 1L;
                } else {
                    long sequence = Tuple.fromBytes(value).getLong(0) + 1;
                    txn.set(sequenceSubspaceKey, Tuple.from(sequence).pack());
                    return sequence;
                }
            }).join();
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
    @Nullable
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReference(@Nonnull final String name) {
        LOGGER.trace("getFDBLuceneFileReference {}", name);
        FDBLuceneFileReference fileReference = this.fileReferenceCache.getIfPresent(name);
        if (fileReference == null) {
            return txn.get(metaSubspace.pack(name))
                    .thenApplyAsync((value) -> {
                            FDBLuceneFileReference fetchedref = value == null ? null : new FDBLuceneFileReference(Tuple.fromBytes(value));
                            if (fetchedref != null) {
                                this.fileReferenceCache.put(name, fetchedref);
                            }
                            return fetchedref;
                        }
                    );
        } else {
            return CompletableFuture.completedFuture(fileReference);
        }
    }

    /**
     * Puts a file reference in the meta subspace and in the cache under the given name.
     * @param name name for the file reference
     * @param reference the file reference being inserted
     */
    public void writeFDBLuceneFileReference(@Nonnull final String name, @Nonnull final FDBLuceneFileReference reference) {
        LOGGER.trace("writeFDBLuceneFileReference {}", reference);
        txn.set(metaSubspace.pack(name), reference.getTuple().pack());
        fileReferenceCache.put(name, reference);
    }

    /**
     * Writes data to the given block under the given id.
     * @param id id for the data
     * @param block block for the data to be stored in
     * @param value the data to be stored
     */
    public void writeData(long id, int block, @Nonnull byte[] value) {
        LOGGER.trace("writeData id={}, block={}, valueSize={}", id, block, value.length);
        Verify.verify(value.length <= blockSize);
        txn.set(dataSubspace.pack(Tuple.from(id, block)), value);
    }

    /**
     * Reads known data from the directory.
     * @param resourceDescription Description should be non-null, opaque string describing this resource; used for logging
     * @param referenceFuture the reference where the data supposedly lives
     * @param block the block where the data is stored
     * @return Completable future of the data returned
     * @throws IOException if blockCache fails to get the data from the block
     * @throws NullPointerException if a reference with that id hasn't been written yet.
     */
    @Nonnull
    public CompletableFuture<byte[]> readBlock(@Nonnull String resourceDescription, @Nonnull CompletableFuture<FDBLuceneFileReference> referenceFuture, int block) throws IOException {
        try {
            LOGGER.trace("readBlock resourceDescription={}, block={}", resourceDescription, block);
            final FDBLuceneFileReference reference = referenceFuture.join(); // Tried to fully pipeline this but the reality is that this is mostly cached after listAll, delete, etc.
            Long id = reference.getId();
            return blockCache.get(Pair.of(id, block),
                    () -> {
                        Long value = reads.getOrDefault(resourceDescription + ":" + id, 0L);
                        value += 1;
                        reads.put(resourceDescription + ":" + id, value);
                    return txn.get(dataSubspace.pack(Tuple.from(id, block)));
                }
            );
        } catch (ExecutionException e) {
            throw new IOException(e);
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
        List<String> outList = new ArrayList<>();
        List<String> displayList = null;

        long totalSize = 0L;
        for (KeyValue kv : txn.getRange(metaSubspace.range())) {
            String name = metaSubspace.unpack(kv.getKey()).getString(0);
            outList.add(name);
            FDBLuceneFileReference fileReference = new FDBLuceneFileReference(Tuple.fromBytes(kv.getValue()));
            // Only composite files and segments are prefetched.
            if (name.endsWith(".cfs") || name.endsWith(".si") || name.endsWith(".cfe")) {
                try {
                    readBlock(name, CompletableFuture.completedFuture(fileReference), 0);
                } catch (IOException ioe) {
                    LOGGER.warn("Cannot Prefetch resource={}", name);
                    LOGGER.warn("Prefetch Error", ioe);
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
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("listAllFiles -> count={}, totalSize={}", outList.size(), totalSize);
            LOGGER.debug("Files -> {}", displayList);
        }

        return outList.toArray(new String[0]);
    }

    /**
     * deletes the file reference under the provided name.
     * @param name the name for the file reference
     */
    @Override
    public void deleteFile(@Nonnull String name) throws IOException {
        LOGGER.trace("deleteFile -> {}", name);
        boolean deleted = getFDBLuceneFileReference(name).thenApplyAsync(
                (value) -> {
                    if (value == null) {
                        return false;
                    }
                    txn.clear(metaSubspace.pack(name));
                    txn.clear(dataSubspace.subspace(Tuple.from(value.getId())).range());
                    this.fileReferenceCache.invalidate(name);
                    return true;
                }
        ).join();
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
        LOGGER.trace("fileLength -> {}", name);
        FDBLuceneFileReference reference = getFDBLuceneFileReference(name).join();
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
        LOGGER.trace("createOutput -> {}", name);
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
        LOGGER.trace("createTempOutput -> prefix={}, suffix={}", prefix, suffix);
        return createOutput(getTempFileName(prefix, suffix, this.nextTempFileCounter.getAndIncrement()), ioContext);
    }

    @Nonnull
    protected static String getTempFileName(@Nonnull String prefix, @Nonnull String suffix, long counter) {
        return IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(counter, 36), "tmp");
    }

    @Override
    public void sync(@Nonnull final Collection<String> collection) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("sync -> {}", String.join(", ", collection));
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        LOGGER.trace("syncMetaData");
    }

    /**
     * It is the caller's responsibility to make the dest (destination) does not exist.
     *
     * @param source source
     * @param dest desc
     */
    @Override
    public void rename(@Nonnull final String source, @Nonnull final String dest) {
        LOGGER.trace("rename -> source={}, dest={}", source, dest);
        final byte[] key = metaSubspace.pack(source);
        txn.get(key).thenAcceptAsync( (value) -> {
            this.fileReferenceCache.invalidate(source);
            txn.set(metaSubspace.pack(dest), value);
            txn.clear(key);
        }).join();
    }

    @Override
    @Nonnull
    public IndexInput openInput(@Nonnull final String name, @Nonnull final IOContext ioContext) throws IOException {
        LOGGER.trace("openInput -> name={}", name);
        return new FDBIndexInput(name, this);
    }

    @Override
    @Nonnull
    public Lock obtainLock(@Nonnull final String lockName) throws IOException {
        LOGGER.trace("obtainLock -> {}", lockName);
        return lockFactory.obtainLock(null, lockName);
    }

    /**
     * No Op that reports in debug mode the block and file reference cache stats.
     */
    @Override
    public void close() {
        LOGGER.debug("close called blockCacheStats={}, referenceCacheStats={}", blockCache.stats(), fileReferenceCache.stats());
    }

    /**
     * We delete inline vs. batching them together.
     *
     * @return Emtpy set of strings
     */
    @Override
    @Nonnull
    public Set<String> getPendingDeletions() {
        LOGGER.trace("getPendingDeletions");
        return Collections.emptySet();
    }

    public int getBlockSize() {
        return blockSize;
    }

    public Transaction getTxn() {
        return txn;
    }

    public Subspace getSubspace() {
        return subspace;
    }
}

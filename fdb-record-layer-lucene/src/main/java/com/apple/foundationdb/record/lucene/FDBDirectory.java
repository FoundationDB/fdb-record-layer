/*
 * FDBDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
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

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
 *
 */
public class FDBDirectory extends Directory {
    private static final Logger LOG = LoggerFactory.getLogger(FDBDirectory.class);
    public static final int DEFAULT_BLOCK_SIZE = 16_384;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    public Transaction txn;
    public final Subspace subspace;
    private final Subspace metaSubspace;
    private final Subspace dataSubspace;
    private final byte[] sequenceSubspaceKey;

    private final LockFactory lockFactory;
    private final int blockSize;
    private static final Set<String> EMPTY_SET = new HashSet<>();
    private AtomicLong atomicLong;
    private boolean hasWritten;
    private CompletableFuture<Void> setIncrement;
    //TODO:  make visible for testing
    private final Cache<String, FDBLuceneFileReference> fileReferenceCache;
    private final Cache<Pair<Long, Integer>, CompletableFuture<byte[]>> blockCache;
    private final Map<String, Long> reads;

    public FDBDirectory(Subspace subspace, Transaction txn) {
        this(subspace, txn, NoLockFactory.INSTANCE);
    }

    FDBDirectory(Subspace subspace, Transaction txn, LockFactory lockFactory) {
        this(subspace, txn, lockFactory, DEFAULT_BLOCK_SIZE);
    }

    FDBDirectory(Subspace subspace, Transaction txn, LockFactory lockFactory, int blockSize) {
        assert subspace != null;
        assert txn != null;
        assert lockFactory != null;
        this.txn = txn;
        this.subspace = subspace;
        final Subspace sequenceSubspace = subspace.subspace(Tuple.from("s"));
        this.sequenceSubspaceKey = sequenceSubspace.pack();
        this.metaSubspace = subspace.subspace(Tuple.from("m"));
        this.dataSubspace = subspace.subspace(Tuple.from("d"));
        this.lockFactory = lockFactory;
        this.blockSize = blockSize;
        this.fileReferenceCache = CacheBuilder.newBuilder().initialCapacity(128).maximumSize(1024).recordStats().build();
        this.blockCache = CacheBuilder.newBuilder().concurrencyLevel(16).initialCapacity(128).maximumSize(1024).recordStats().build();
        this.reads = new ConcurrentHashMap<>();
    }

    /**.
     * Sets increment if not set yet.
     * @return Future representing the status of setting the increment
     */
    private CompletableFuture<Void> setIncrement() {
        return txn.get(sequenceSubspaceKey).thenAcceptAsync(
                (value) -> {
                    if (value == null) {
                        atomicLong = new AtomicLong(1L);
                    } else {
                        long sequence = Tuple.fromBytes(value).getLong(0);
                        atomicLong = new AtomicLong(sequence + 1);
                    }
                });
    }

    /**
     * Sets increment if not set yet and waits till completed to return
     * Returns and increments the increment if its already set.
     * @return current increment value
     */
    public long getIncrement() {
        if (setIncrement == null) {
            setIncrement = setIncrement();
            setIncrement.join();
            return atomicLong.get();
        }
        setIncrement.join();
        return atomicLong.incrementAndGet();
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
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReference(final String name) {
        LOG.trace("getFDBLuceneFileReference {}", name);
        FDBLuceneFileReference fileReference = this.fileReferenceCache.getIfPresent(name);
        return fileReference == null ? txn.get(metaSubspace.pack(name))
                    .thenApplyAsync( (value) -> {
                                FDBLuceneFileReference  fetchedref = value == null ? null : new FDBLuceneFileReference(Tuple.fromBytes(value));
                        if (fetchedref != null) {
                            this.fileReferenceCache.put(name, fetchedref);
                        }
                        return fetchedref;
                        }
                    ) : CompletableFuture.supplyAsync(() -> fileReference);
    }

    /**
     * Puts a file reference in the meta subspace and in the cache under the given name.
     * @param name name for the file reference
     * @param reference the file reference being inserted
     * TODO: this overwrites the file reference under that name, expected behavior?
     */
    public void writeFDBLuceneFileReference(final String name, final FDBLuceneFileReference reference) {
        LOG.trace("writeFDBLuceneFileReference {}", reference);
        hasWritten = true;
        txn.set(metaSubspace.pack(name), reference.getTuple().pack());
        fileReferenceCache.put(name, reference);
    }

    /**
     * Writes data to the given block under the given id.
     * @param id id for the data
     * @param block block for the data to be stored in
     * @param value the data to be stored
     */
    public void writeData(long id, int block, byte[] value) {
        LOG.trace("writeData id={}, block={}, valueSize={}", id, block, value.length);
        hasWritten = true;
        txn.set(dataSubspace.pack(Tuple.from(id, block)), value);
    }

    /**
     * Seeks known data from the directory.
     * @param resourceDescription TODO: Is there anywhere else this is referenced? I don't understand what this is.
     * @param referenceFuture the reference where the data supposedly lives
     * @param block the block where the data is stored
     * @return Completable future of the data returned
     * @throws IOException if blockCache fails to get the data from the block
     * @throws NullPointerException if a reference with that id hasn't been written yet.
     */
    public CompletableFuture<byte[]> seekData(String resourceDescription, CompletableFuture<FDBLuceneFileReference> referenceFuture, int block) throws IOException {
        try {
            LOG.trace("seekData resourceDescription={}, block={}", resourceDescription, block);
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
     * Lists all references in the subspace. Puts all references in the cache.
     * Logs the count of references, and the total size of the data.
     * @return String list of names of lucene file references
     * TODO: Why do we add all to the cache.
     * Doesn't that defeat the point of the cache if we make it too large?
     * Wouldn't it cost just as much to search the data if we've added everything to the cache?
     * (I understand on hard disk this is different but in a solidstate/cloud model does this make a difference?)
     */
    @Override
    public String[] listAll() {
        List<String> outList = new ArrayList<>();
        List<String> displayList = new ArrayList<>();
        long totalSize = 0L;
        for (KeyValue kv : txn.getRange(metaSubspace.range())) {
            String name = metaSubspace.unpack(kv.getKey()).getString(0);
            outList.add(name);
            FDBLuceneFileReference fileReference = new FDBLuceneFileReference(Tuple.fromBytes(kv.getValue()));
            if (name.endsWith(".cfs") || name.endsWith(".si") || name.endsWith(".cfe")) {
                try {
                    seekData(name, CompletableFuture.completedFuture(fileReference), 0);
                } catch (IOException ioe) {
                    LOG.warn("Cannot Prefetch resource={}", name);
                    LOG.warn("Prefetch Error", ioe);
                }
            }
            this.fileReferenceCache.put(name, fileReference);
            if (LOG.isDebugEnabled()) {
                if (kv.getValue() != null) {
                    displayList.add(name + "(" + fileReference.getSize() + ")");
                    totalSize += fileReference.getSize();
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("listAllFiles -> count={}, totalSize={}", outList.size(), totalSize);
            LOG.debug("Files -> {}", displayList);
        }

        return outList.toArray(new String[0]);
    }

    /**
     * deletes the file reference under the provided name.
     * @param name the name for the file reference
     */
    @Override
    public void deleteFile(String name) {
        LOG.trace("deleteFile -> {}", name);
        hasWritten = true;
        getFDBLuceneFileReference(name).thenAcceptAsync(
                (value) -> {
                    if (value == null) {
                        throw new RuntimeException(new NoSuchFileException(name));
                    }
                    txn.clear(metaSubspace.pack(name));
                    txn.clear(dataSubspace.subspace(Tuple.from(value.getId())).range());
                    this.fileReferenceCache.invalidate(name);
                }
        ).join();
    }

    /**
     * Returns the size of the given file under the given name.
     * @param name the name of the file reference
     * @return long value of the size of the file
     * @throws NoSuchFileException if the file reference doesn't exist.
     */
    @Override
    public long fileLength(String name) throws NoSuchFileException {
        LOG.trace("fileLength -> {}", name);
        FDBLuceneFileReference reference = getFDBLuceneFileReference(name).join();
        if (reference == null) {
            throw new NoSuchFileException(name);
        }
        return reference.getSize();
    }

    /**
     * Create new output for
     * @param name
     * @param ioContext
     * @return
     */
    @Override
    public IndexOutput createOutput(final String name, final IOContext ioContext) {
        LOG.trace("createOutput -> {}", name);
        hasWritten = true;
        return new FDBIndexOutput(name, name, this);
    }

    @Override
    public IndexOutput createTempOutput(final String prefix, final String suffix, final IOContext ioContext) {
        LOG.trace("createTempOutput -> prefix={}, suffix={}", prefix, suffix);
        hasWritten = true;
        return createOutput(getTempFileName(prefix, suffix, this.nextTempFileCounter.getAndIncrement()), ioContext);
    }

    protected static String getTempFileName(String prefix, String suffix, long counter) {
        return IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(counter, 36), "tmp");
    }

    @Override
    public void sync(final Collection<String> collection) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("sync -> {}", String.join(", ", collection));
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        LOG.trace("syncMetaData hasWritten={}",hasWritten);
        if (hasWritten) {
            txn.set(sequenceSubspaceKey, Tuple.from(atomicLong.getAndIncrement()).pack());
        }
    }

    @Override
    public void rename(final String source, final String dest) throws IOException {
        LOG.trace("rename -> source={}, dest={}", source, dest);
        final byte[] key = metaSubspace.pack(source);
        // TODO: probably makes sense to make sure that the key isn't null.
        //  Otherwise we get Illegal argument from a non-obvious place when we try to pack it.
        txn.get(key).thenAcceptAsync( (value) -> {
            txn.set(metaSubspace.pack(dest), value);
            txn.clear(key);
        }).join();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext ioContext) throws IOException {
        LOG.trace("openInput -> name={}", name);
        return new FDBIndexInput(name, this);
    }

    @Override
    public Lock obtainLock(final String s) throws IOException {
        LOG.trace("obtainLock -> {}", s);
        return lockFactory.obtainLock(null,s);
    }

    /**
     * TODO: verify if anything else needs to happen here.
     * All this is doing is logging not actually closing anything.
     * Close implies that it shouldn't be used again.
     * @throws IOException TODO: no it doesn't?
     */
    @Override
    public void close() throws IOException {
        LOG.debug("close");
        LOG.debug("Block Cache stats={}", blockCache.stats());
        LOG.debug("Reference Cache stats={}", fileReferenceCache.stats());
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        LOG.trace("getPendingDeletions");
        return EMPTY_SET;
    }

    public int getBlockSize() {
        return blockSize;
    }
}

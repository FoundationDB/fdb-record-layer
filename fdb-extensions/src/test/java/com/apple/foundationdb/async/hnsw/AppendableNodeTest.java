/*
 * AppendableNodeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.foundationdb.async.common.CommonTestHelpers.randomVectors;
import static com.apple.foundationdb.async.common.CommonTestHelpers.runAsyncToSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NodeKind#COMPACT_APPENDABLE}, i.e. for the format that allows adding a neighbor to a compact node
 * by appending to the stored value. Apart from testing the (de-)serialization and the upgrade path from
 * {@link NodeKind#COMPACT}, this test quantifies how much smaller the writes of inserting into an {@link HNSW} get
 * when using that format.
 */
@Tag(Tags.RequiresFDB)
class AppendableNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(AppendableNodeTest.class);

    private static final int NUM_DIMENSIONS = 128;
    private static final int NUM_NEIGHBORS = 20;
    private static final int FDB_VALUE_TOO_LARGE = 2103;
    private static final int FDB_NOT_COMMITTED = 1020;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @Nonnull
    private static Config config(final boolean writeAppendableNodes) {
        return HNSW.newConfigBuilder()
                .setEfConstruction(100)
                .setWriteAppendableNodes(writeAppendableNodes)
                .build(NUM_DIMENSIONS);
    }

    @Nonnull
    private CompactStorageAdapter storageAdapter(final boolean writeAppendableNodes,
                                                 @Nonnull final OnWriteListener onWriteListener) {
        return new CompactStorageAdapter(config(writeAppendableNodes), CompactNode.factory(),
                subspaceExtension.getSubspace(), onWriteListener, OnReadListener.NOOP);
    }

    @Nonnull
    private CompactNode fetch(@Nonnull final CompactStorageAdapter adapter, final int layer,
                              @Nonnull final Tuple primaryKey) throws Exception {
        return runAsyncToSync(dbExtension.getDatabase(),
                tr -> adapter.fetchNode(tr, StorageTransform.identity(), layer, primaryKey)).asCompactNode();
    }

    @Nonnull
    private CompactNode fetch(@Nonnull final CompactStorageAdapter adapter, @Nonnull final Tuple primaryKey)
            throws Exception {
        return fetch(adapter, 0, primaryKey);
    }

    private static void write(@Nonnull final Transaction transaction, @Nonnull final CompactStorageAdapter adapter,
                              final int layer, @Nonnull final AbstractNode<NodeReference> node,
                              @Nonnull final NeighborsChangeSet<NodeReference> changeSet) {
        adapter.writeNode(transaction, Quantizer.noOpQuantizer(Metric.EUCLIDEAN_METRIC), layer, node, changeSet);
    }

    /**
     * Inserts neighbors into the node the way the graph does it.
     */
    private static void insertNeighbors(@Nonnull final Database db, @Nonnull final CompactStorageAdapter adapter,
                                        final int layer, @Nonnull final AbstractNode<NodeReference> node,
                                        @Nonnull final List<NodeReference> inserted) {
        db.run(tr -> {
            write(tr, adapter, layer, node,
                    new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(node.getNeighbors()), inserted));
            return null;
        });
    }

    private static void writeNew(@Nonnull final Database db, @Nonnull final CompactStorageAdapter adapter,
                                 final int layer, @Nonnull final AbstractNode<NodeReference> node) {
        db.run(tr -> {
            write(tr, adapter, layer, node,
                    new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                            node.getNeighbors()));
            return null;
        });
    }

    @Nonnull
    private static List<NodeReference> randomNeighbors(@Nonnull final Random random, final int num) {
        final ImmutableList.Builder<NodeReference> builder = ImmutableList.builder();
        for (int i = 0; i < num; i ++) {
            builder.add(TestHelpers.createRandomNodeReference(random));
        }
        return builder.build();
    }

    @Nonnull
    private static AbstractNode<NodeReference> randomNode(@Nonnull final Random random,
                                                          @Nonnull final CompactStorageAdapter adapter,
                                                          @Nullable final Tuple additionalValues) {
        final AbstractNode<NodeReference> node =
                TestHelpers.createRandomCompactNode(random, adapter.getNodeFactory(), NUM_DIMENSIONS, NUM_NEIGHBORS);
        return adapter.getNodeFactory().create(node.getPrimaryKey(), node.asCompactNode().getVector(),
                additionalValues, node.getNeighbors());
    }

    /**
     * The number of bytes the listener is told about when neighbors are appended: the key and the packed neighbors.
     */
    private static long expectedAppendBytes(@Nonnull final CompactStorageAdapter adapter, final int layer,
                                            @Nonnull final Tuple primaryKey,
                                            @Nonnull final List<NodeReference> inserted) {
        final List<Tuple> insertedPrimaryKeys = ImmutableList.copyOf(
                inserted.stream().map(NodeReference::getPrimaryKey).iterator());
        return adapter.getDataSubspace().pack(Tuple.from(layer, primaryKey)).length
                + Tuple.fromList(insertedPrimaryKeys).pack().length;
    }

    private static int fdbErrorCode(@Nonnull final Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current instanceof FDBException) {
                return ((FDBException)current).getCode();
            }
        }
        throw new AssertionError("no FDBException in cause chain of " + throwable);
    }

    @Nonnull
    private static Tuple additionalValuesOfSize(final int size) {
        final byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte)1);
        return Tuple.from((Object)bytes);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testSerializationRoundTrip(final long seed) throws Exception {
        final Random random = new Random(seed);
        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(dbExtension.getDatabase(), adapter, 0, node);

        final CompactNode fetched = fetch(adapter, node.getPrimaryKey());
        assertThat(fetched.getKind()).isSameAs(NodeKind.COMPACT);
        assertThat(fetched.isAppendable()).isTrue();
        assertThat(fetched.getVector()).isEqualTo(node.asCompactNode().getVector());
        assertThat((Object)fetched.getAdditionalValues()).isNull();
        assertThat(fetched.getNeighbors()).containsExactlyElementsOf(node.getNeighbors());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testSerializationRoundTripWithAdditionalValues(final long seed) throws Exception {
        final Random random = new Random(seed);
        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);
        final Tuple additionalValues = Tuple.from("a", 1L, new byte[] {1, 2, 3});

        final AbstractNode<NodeReference> node = randomNode(random, adapter, additionalValues);
        writeNew(dbExtension.getDatabase(), adapter, 0, node);

        final CompactNode fetched = fetch(adapter, node.getPrimaryKey());
        assertThat(fetched.isAppendable()).isTrue();
        assertThat((Object)fetched.getAdditionalValues()).isEqualTo(additionalValues);
        assertThat(fetched.getNeighbors()).containsExactlyElementsOf(node.getNeighbors());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testAppendWritesOnlyTheInsertedNeighbors(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter adapter = storageAdapter(true, listener);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, 0, node);
        final long fullWriteBytes = listener.takeBytesWritten();

        final List<NodeReference> inserted = randomNeighbors(random, 2);
        insertNeighbors(db, adapter, 0, fetch(adapter, node.getPrimaryKey()), inserted);

        assertThat(listener.takeBytesWritten())
                .isEqualTo(expectedAppendBytes(adapter, 0, node.getPrimaryKey(), inserted))
                .isLessThan(fullWriteBytes);
        final CompactNode afterAppend = fetch(adapter, node.getPrimaryKey());
        assertThat(afterAppend.isAppendable()).isTrue();
        assertThat(afterAppend.getVector()).isEqualTo(node.asCompactNode().getVector());
        assertThat(afterAppend.getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).addAll(inserted).build());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testAppendKeepsAdditionalValues(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);
        final Tuple additionalValues = Tuple.from("a", 1L);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, additionalValues);
        writeNew(db, adapter, 0, node);
        final List<NodeReference> inserted = randomNeighbors(random, 3);
        insertNeighbors(db, adapter, 0, fetch(adapter, node.getPrimaryKey()), inserted);

        final CompactNode afterAppend = fetch(adapter, node.getPrimaryKey());
        assertThat((Object)afterAppend.getAdditionalValues()).isEqualTo(additionalValues);
        assertThat(afterAppend.getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).addAll(inserted).build());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testAppendOnUpperLayer(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter adapter = storageAdapter(true, listener);
        final int layer = 3;

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, layer, node);
        listener.takeBytesWritten();
        final List<NodeReference> inserted = randomNeighbors(random, 2);
        insertNeighbors(db, adapter, layer, fetch(adapter, layer, node.getPrimaryKey()), inserted);

        assertThat(listener.takeBytesWritten())
                .isEqualTo(expectedAppendBytes(adapter, layer, node.getPrimaryKey(), inserted));
        assertThat(fetch(adapter, layer, node.getPrimaryKey()).getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).addAll(inserted).build());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testTwoAppendsInOneTransaction(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, 0, node);
        final NodeReference first = TestHelpers.createRandomNodeReference(random);
        final NodeReference second = TestHelpers.createRandomNodeReference(random);

        db.run(tr -> {
            final CompactNode read = adapter.fetchNode(tr, StorageTransform.identity(), 0, node.getPrimaryKey())
                    .join().asCompactNode();
            write(tr, adapter, 0, read,
                    new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(read.getNeighbors()),
                            ImmutableList.of(first)));
            // reading our own write yields the node with the appended neighbor and its updated size
            final CompactNode reread = adapter.fetchNode(tr, StorageTransform.identity(), 0, node.getPrimaryKey())
                    .join().asCompactNode();
            assertThat(reread.getNeighbors()).hasSize(NUM_NEIGHBORS + 1);
            write(tr, adapter, 0, reread,
                    new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(reread.getNeighbors()),
                            ImmutableList.of(second)));
            return null;
        });

        assertThat(fetch(adapter, node.getPrimaryKey()).getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).add(first).add(second).build());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testReplacingExistingNeighborRewritesWithoutDuplicate(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter adapter = storageAdapter(true, listener);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, 0, node);
        final long fullWriteBytes = listener.takeBytesWritten();

        final CompactNode fetched = fetch(adapter, node.getPrimaryKey());
        final NodeReference existing = fetched.getNeighbors().get(3);
        final NeighborsChangeSet<NodeReference> replace =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(fetched.getNeighbors()),
                        ImmutableList.of(existing));
        assertThat(replace.getPureInsertions()).isNull();
        insertNeighbors(db, adapter, 0, fetched, ImmutableList.of(existing));

        // the node was rewritten in its entirety, which has the same size as the original write
        assertThat(listener.takeBytesWritten()).isEqualTo(fullWriteBytes);
        assertThat(fetch(adapter, node.getPrimaryKey()).getNeighbors()).hasSameSizeAs(fetched.getNeighbors());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testDeleteCombinedWithInsertRewrites(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter adapter = storageAdapter(true, listener);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, 0, node);
        final long fullWriteBytes = listener.takeBytesWritten();

        final CompactNode fetched = fetch(adapter, node.getPrimaryKey());
        final NodeReference deleted = fetched.getNeighbors().get(3);
        final NodeReference inserted = TestHelpers.createRandomNodeReference(random);
        final NeighborsChangeSet<NodeReference> deleteAndInsert =
                new InsertNeighborsChangeSet<>(
                        new DeleteNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(fetched.getNeighbors()),
                                ImmutableList.of(deleted.getPrimaryKey())),
                        ImmutableList.of(inserted));
        assertThat(deleteAndInsert.getPureInsertions()).isNull();
        db.run(tr -> {
            write(tr, adapter, 0, fetched, deleteAndInsert);
            return null;
        });

        assertThat(listener.takeBytesWritten()).isEqualTo(fullWriteBytes);
        assertThat(fetch(adapter, node.getPrimaryKey()).getNeighbors())
                .extracting(NodeReference::getPrimaryKey)
                .doesNotContain(deleted.getPrimaryKey())
                .contains(inserted.getPrimaryKey());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testNoChangesRewrites(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter adapter = storageAdapter(true, listener);

        final AbstractNode<NodeReference> node = randomNode(random, adapter, null);
        writeNew(db, adapter, 0, node);
        final long fullWriteBytes = listener.takeBytesWritten();

        insertNeighbors(db, adapter, 0, fetch(adapter, node.getPrimaryKey()), ImmutableList.of());

        assertThat(listener.takeBytesWritten()).isEqualTo(fullWriteBytes);
        assertThat(fetch(adapter, node.getPrimaryKey()).getNeighbors())
                .containsExactlyElementsOf(node.getNeighbors());
    }

    /**
     * {@code APPEND_IF_FITS} does nothing, silently, if the result does not fit. The adapter must therefore not
     * append in that case. It writes the node in its entirety instead, which FDB rejects as being too large: the
     * failure is loud and leaves the node as it was, rather than losing the neighbor.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void testAppendThatDoesNotFitFailsLoudly(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);

        // the size of the value is linear in the size of the additional values as long as there are no zero bytes, so
        // write the node once to learn the size of everything else and then size the additional values to fit
        final int probeAdditionalValuesSize = 1000;
        final AbstractNode<NodeReference> probe =
                randomNode(random, adapter, additionalValuesOfSize(probeAdditionalValuesSize));
        writeNew(db, adapter, 0, probe);
        final int probeSize = fetch(adapter, probe.getPrimaryKey()).getAppendableValueSize();

        final int targetSize = CompactStorageAdapter.MAX_APPENDABLE_VALUE_SIZE - 1;
        final AbstractNode<NodeReference> node = adapter.getNodeFactory().create(probe.getPrimaryKey(),
                probe.asCompactNode().getVector(),
                additionalValuesOfSize(probeAdditionalValuesSize + targetSize - probeSize), probe.getNeighbors());
        writeNew(db, adapter, 0, node);
        final CompactNode fetched = fetch(adapter, node.getPrimaryKey());
        assertThat(fetched.getAppendableValueSize()).isEqualTo(targetSize);

        assertThatThrownBy(() -> insertNeighbors(db, adapter, 0, fetched, randomNeighbors(random, 1)))
                .satisfies(e -> assertThat(fdbErrorCode(e)).isEqualTo(FDB_VALUE_TOO_LARGE));

        final CompactNode afterFailure = fetch(adapter, node.getPrimaryKey());
        assertThat(afterFailure.getAppendableValueSize()).isEqualTo(targetSize);
        assertThat(afterFailure.getNeighbors()).containsExactlyElementsOf(node.getNeighbors());
    }

    /**
     * An append is only safe if the stored value is the one that the node was read from. The read of the node adds a
     * read conflict, so a concurrent rewrite causes the appending transaction to fail rather than to corrupt the node.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testAppendConflictsWithConcurrentRewrite(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter appendableAdapter = storageAdapter(true, OnWriteListener.NOOP);
        final CompactStorageAdapter oldAdapter = storageAdapter(false, OnWriteListener.NOOP);

        final AbstractNode<NodeReference> node = randomNode(random, appendableAdapter, null);
        writeNew(db, appendableAdapter, 0, node);

        try (Transaction tr = db.createTransaction()) {
            final CompactNode read = appendableAdapter.fetchNode(tr, StorageTransform.identity(), 0,
                    node.getPrimaryKey()).join().asCompactNode();
            assertThat(read.isAppendable()).isTrue();

            // another transaction converts the node to the old format
            final NodeReference concurrentlyInserted = TestHelpers.createRandomNodeReference(random);
            insertNeighbors(db, oldAdapter, 0, read, ImmutableList.of(concurrentlyInserted));

            write(tr, appendableAdapter, 0, read,
                    new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(read.getNeighbors()),
                            ImmutableList.of(TestHelpers.createRandomNodeReference(random))));
            assertThatThrownBy(() -> tr.commit().join())
                    .satisfies(e -> assertThat(fdbErrorCode(e)).isEqualTo(FDB_NOT_COMMITTED));

            final CompactNode stored = fetch(appendableAdapter, node.getPrimaryKey());
            assertThat(stored.isAppendable()).isFalse();
            assertThat(stored.getNeighbors())
                    .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                            .addAll(node.getNeighbors()).add(concurrentlyInserted).build());
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testBothFormatsAreReadable(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter oldAdapter = storageAdapter(false, OnWriteListener.NOOP);
        final CompactStorageAdapter newAdapter = storageAdapter(true, OnWriteListener.NOOP);

        final AbstractNode<NodeReference> oldNode = randomNode(random, oldAdapter, Tuple.from("a"));
        writeNew(db, oldAdapter, 0, oldNode);
        final AbstractNode<NodeReference> newNode = randomNode(random, newAdapter, Tuple.from("b"));
        writeNew(db, newAdapter, 0, newNode);

        for (final CompactStorageAdapter reader : ImmutableList.of(oldAdapter, newAdapter)) {
            final CompactNode readOld = fetch(reader, oldNode.getPrimaryKey());
            assertThat(readOld.isAppendable()).isFalse();
            assertThat((Object)readOld.getAdditionalValues()).isEqualTo(Tuple.from("a"));
            assertThat(readOld.getNeighbors()).containsExactlyElementsOf(oldNode.getNeighbors());

            final CompactNode readNew = fetch(reader, newNode.getPrimaryKey());
            assertThat(readNew.isAppendable()).isTrue();
            assertThat((Object)readNew.getAdditionalValues()).isEqualTo(Tuple.from("b"));
            assertThat(readNew.getNeighbors()).containsExactlyElementsOf(newNode.getNeighbors());
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testInsertIntoOldFormatNodeUpgradesIt(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter oldAdapter = storageAdapter(false, OnWriteListener.NOOP);
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final CompactStorageAdapter newAdapter = storageAdapter(true, listener);

        final AbstractNode<NodeReference> node = randomNode(random, oldAdapter, null);
        writeNew(db, oldAdapter, 0, node);

        // appending to the old format would corrupt the node, so the node is rewritten in the new format ...
        final NodeReference first = TestHelpers.createRandomNodeReference(random);
        insertNeighbors(db, newAdapter, 0, fetch(newAdapter, node.getPrimaryKey()), ImmutableList.of(first));
        final CompactNode upgraded = fetch(newAdapter, node.getPrimaryKey());
        assertThat(upgraded.isAppendable()).isTrue();
        assertThat(upgraded.getVector()).isEqualTo(node.asCompactNode().getVector());
        assertThat(upgraded.getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).add(first).build());

        // ... after which it can be appended to
        listener.takeBytesWritten();
        final List<NodeReference> second = randomNeighbors(random, 1);
        insertNeighbors(db, newAdapter, 0, upgraded, second);
        assertThat(listener.takeBytesWritten())
                .isEqualTo(expectedAppendBytes(newAdapter, 0, node.getPrimaryKey(), second));
        assertThat(fetch(newAdapter, node.getPrimaryKey()).getNeighbors()).hasSize(NUM_NEIGHBORS + 2);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void testInsertIntoAppendableNodeWithoutWritingAppendableDowngradesIt(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final CompactStorageAdapter oldAdapter = storageAdapter(false, OnWriteListener.NOOP);
        final CompactStorageAdapter newAdapter = storageAdapter(true, OnWriteListener.NOOP);

        final AbstractNode<NodeReference> node = randomNode(random, newAdapter, null);
        writeNew(db, newAdapter, 0, node);

        final CompactNode appendable = fetch(oldAdapter, node.getPrimaryKey());
        assertThat(appendable.isAppendable()).isTrue();
        final NodeReference inserted = TestHelpers.createRandomNodeReference(random);
        insertNeighbors(db, oldAdapter, 0, appendable, ImmutableList.of(inserted));

        final CompactNode downgraded = fetch(oldAdapter, node.getPrimaryKey());
        assertThat(downgraded.isAppendable()).isFalse();
        assertThat(downgraded.getNeighbors())
                .containsExactlyElementsOf(ImmutableList.<NodeReference>builder()
                        .addAll(node.getNeighbors()).add(inserted).build());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void testGraphWithUpgradeInTheMiddle(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final int size = 300;
        final List<PrimaryKeyAndVector> data = randomVectors(random, NUM_DIMENSIONS, size);

        final HNSW before = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                config(false), OnWriteListener.NOOP, OnReadListener.NOOP);
        final HNSW after = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                config(true), OnWriteListener.NOOP, OnReadListener.NOOP);
        for (int i = 0; i < size; i ++) {
            final HNSW hnsw = i < size / 2 ? before : after;
            final PrimaryKeyAndVector record = data.get(i);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
        }

        // every vector finds itself, no matter which format its node and the nodes around it are stored in
        for (final PrimaryKeyAndVector record : data) {
            assertFindsItself(db, after, record);
        }
    }

    /**
     * Deleting repairs the neighborhood of the deleted node, which removes neighbors and thus must rewrite nodes
     * rather than append to them.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void testGraphWithDeletes(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final int size = 200;
        final List<PrimaryKeyAndVector> data = randomVectors(random, NUM_DIMENSIONS, size);

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                config(true), OnWriteListener.NOOP, OnReadListener.NOOP);
        for (final PrimaryKeyAndVector record : data) {
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
        }
        for (final PrimaryKeyAndVector record : data.subList(0, size / 2)) {
            runAsyncToSync(db, tr -> hnsw.delete(tr, record.primaryKey()));
        }

        for (final PrimaryKeyAndVector record : data.subList(size / 2, size)) {
            assertFindsItself(db, hnsw, record);
        }
        for (final PrimaryKeyAndVector record : data.subList(0, size / 2)) {
            final List<? extends ResultEntry> results =
                    runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, 1, 100, true, record.vector()));
            assertThat(results).hasSize(1);
            assertThat((Object)results.get(0).primaryKey()).isNotEqualTo(record.primaryKey());
        }
    }

    /**
     * The order of the neighbors of a node is not significant: appending adds neighbors at the end whereas a rewrite
     * writes the neighbors in whatever order they are merged in. Shuffling the neighbors of every node must therefore
     * not change what is found.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void testNeighborOrderDoesNotMatter(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Database db = dbExtension.getDatabase();
        final int size = 300;
        final int k = 10;
        final List<PrimaryKeyAndVector> data = randomVectors(random, NUM_DIMENSIONS, size);
        final List<PrimaryKeyAndVector> queries = randomVectors(random, NUM_DIMENSIONS, 20);

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                config(true), OnWriteListener.NOOP, OnReadListener.NOOP);
        for (final PrimaryKeyAndVector record : data) {
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
        }
        final List<List<Tuple>> resultsBefore = search(db, hnsw, queries, k);

        final CompactStorageAdapter adapter = storageAdapter(true, OnWriteListener.NOOP);
        final List<AbstractNode<NodeReference>> nodes = runAsyncToSync(db,
                tr -> adapter.scanLayer(tr, StorageTransform.identity(), 0, null, size).asList());
        assertThat(nodes).hasSize(size);
        db.run(tr -> {
            for (final AbstractNode<NodeReference> node : nodes) {
                final List<NodeReference> shuffled = new ArrayList<>(node.getNeighbors());
                Collections.shuffle(shuffled, random);
                write(tr, adapter, 0, node, new BaseNeighborsChangeSet<>(shuffled));
            }
            return null;
        });
        assertThat(fetch(adapter, nodes.get(0).getPrimaryKey()).getNeighbors())
                .containsExactlyInAnyOrderElementsOf(nodes.get(0).getNeighbors());

        assertThat(search(db, hnsw, queries, k)).isEqualTo(resultsBefore);
    }

    @Nonnull
    private static List<List<Tuple>> search(@Nonnull final Database db, @Nonnull final HNSW hnsw,
                                            @Nonnull final List<PrimaryKeyAndVector> queries, final int k)
            throws Exception {
        final ImmutableList.Builder<List<Tuple>> resultsBuilder = ImmutableList.builder();
        for (final PrimaryKeyAndVector query : queries) {
            final List<? extends ResultEntry> results =
                    runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, true, query.vector()));
            resultsBuilder.add(results.stream().map(ResultEntry::primaryKey).collect(ImmutableList.toImmutableList()));
        }
        return resultsBuilder.build();
    }

    private static void assertFindsItself(@Nonnull final Database db, @Nonnull final HNSW hnsw,
                                          @Nonnull final PrimaryKeyAndVector record) throws Exception {
        final List<? extends ResultEntry> results =
                runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, 1, 100, true, record.vector()));
        assertThat(results).hasSize(1);
        assertThat((Object)results.get(0).primaryKey()).isEqualTo(record.primaryKey());
    }

    /**
     * Inserts the same vectors into two graphs, one using the existing format and one using the appendable format,
     * and compares what is written. This is more of a benchmark than a test; the assertions are deliberately loose.
     * {@link Transaction#getApproximateSize()} is taken just before the commit, which accounts for mutations as well
     * as for the conflict ranges that both formats have in common.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL})
    void quantifyTransactionSize(final long seed) {
        final Database db = dbExtension.getDatabase();
        final int size = 300;
        final List<PrimaryKeyAndVector> data = randomVectors(new Random(seed), NUM_DIMENSIONS, size);

        final Result existing = insertAll(db, data, false, subspaceExtension.getSubspace().subspace(Tuple.from("e")));
        final Result appendable = insertAll(db, data, true, subspaceExtension.getSubspace().subspace(Tuple.from("a")));

        logger.info("numDimensions={} inserts={} existing={} appendable={} transactionSizeRatio={} writtenBytesRatio={}",
                NUM_DIMENSIONS, size, existing, appendable,
                (double)appendable.transactionBytes() / existing.transactionBytes(),
                (double)appendable.writtenBytes() / existing.writtenBytes());

        // most of what the existing format writes are vectors that the appendable format does not need to write again
        assertThat(appendable.writtenBytes()).isLessThan(existing.writtenBytes() / 2);
        assertThat(appendable.transactionBytes()).isLessThanOrEqualTo(existing.transactionBytes());
    }

    @Nonnull
    private static Result insertAll(@Nonnull final Database db, @Nonnull final List<PrimaryKeyAndVector> data,
                                    final boolean writeAppendableNodes, @Nonnull final Subspace subspace) {
        final CountingOnWriteListener listener = new CountingOnWriteListener();
        final HNSW hnsw = new HNSW(subspace, TestExecutors.defaultThreadPool(), config(writeAppendableNodes),
                listener, OnReadListener.NOOP);
        long transactionBytes = 0;
        for (final PrimaryKeyAndVector record : data) {
            transactionBytes += db.run(tr -> {
                hnsw.insert(tr, record.primaryKey(), record.vector(), null).join();
                return tr.getApproximateSize().join();
            });
        }
        return new Result(transactionBytes, listener.getTotalBytesWritten());
    }

    private record Result(long transactionBytes, long writtenBytes) {
    }

    private static class CountingOnWriteListener implements OnWriteListener {
        private final AtomicLong bytesWritten = new AtomicLong();
        private final AtomicLong totalBytesWritten = new AtomicLong();

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            final int length = key.length + value.length;
            bytesWritten.addAndGet(length);
            totalBytesWritten.addAndGet(length);
        }

        long takeBytesWritten() {
            return bytesWritten.getAndSet(0L);
        }

        long getTotalBytesWritten() {
            return totalBytesWritten.get();
        }
    }
}

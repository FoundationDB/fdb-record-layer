/*
 * RTreeModificationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.christianheina.langx.half4j.Half;
import com.google.common.collect.ImmutableList;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class HNSWModificationTest {
    private static final Logger logger = LoggerFactory.getLogger(HNSWModificationTest.class);
    private static final int NUM_TEST_RUNS = 5;
    private static final int NUM_SAMPLES = 10_000;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rtSubspace = new TestSubspaceExtension(dbExtension);
    @RegisterExtension
    TestSubspaceExtension rtSecondarySubspace = new TestSubspaceExtension(dbExtension);

    private Database db;

    @BeforeEach
    public void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @Test
    public void testCompactSerialization() {
        final Random random = new Random(0);
        final CompactStorageAdapter storageAdapter =
                new CompactStorageAdapter(HNSW.DEFAULT_CONFIG, CompactNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReference> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReference> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReference> randomCompactNode =
                            createRandomCompactNode(random, nodeFactory, 768, 16);

                    writeNode(tr, storageAdapter, randomCompactNode, 0);
                    return randomCompactNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, 0, originalNode.getPrimaryKey())
                .thenAccept(node -> {
                    Assertions.assertAll(
                            () -> Assertions.assertInstanceOf(CompactNode.class, node),
                            () -> Assertions.assertEquals(NodeKind.COMPACT, node.getKind()),
                            () -> Assertions.assertEquals(node.getPrimaryKey(), originalNode.getPrimaryKey()),
                            () -> Assertions.assertEquals(node.asCompactNode().getVector(),
                                    originalNode.asCompactNode().getVector()),
                            () -> {
                                final ArrayList<NodeReference> neighbors =
                                        Lists.newArrayList(node.getNeighbors());
                                neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                final ArrayList<NodeReference> originalNeighbors =
                                        Lists.newArrayList(originalNode.getNeighbors());
                                originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                Assertions.assertEquals(neighbors, originalNeighbors);
                            }
                    );
                }).join());
    }

    @Test
    public void testInliningSerialization() {
        final Random random = new Random(0);
        final InliningStorageAdapter storageAdapter =
                new InliningStorageAdapter(HNSW.DEFAULT_CONFIG, InliningNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReferenceWithVector> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReferenceWithVector> randomInliningNode =
                            createRandomInliningNode(random, nodeFactory, 768, 16);

                    writeNode(tr, storageAdapter, randomInliningNode, 0);
                    return randomInliningNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, 0, originalNode.getPrimaryKey())
                .thenAccept(node -> Assertions.assertAll(
                        () -> Assertions.assertInstanceOf(InliningNode.class, node),
                        () -> Assertions.assertEquals(NodeKind.INLINING, node.getKind()),
                        () -> Assertions.assertEquals(node.getPrimaryKey(), originalNode.getPrimaryKey()),
                        () -> {
                            final ArrayList<NodeReference> neighbors =
                                    Lists.newArrayList(node.getNeighbors());
                            neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey)); // should not be necessary the way it is stored
                            final ArrayList<NodeReference> originalNeighbors =
                                    Lists.newArrayList(originalNode.getNeighbors());
                            originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                            Assertions.assertEquals(neighbors, originalNeighbors);
                        }
                )).join());
    }

    @Test
    public void testBasicInsert() {
        final Random random = new Random(0);
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool());

        db.run(tr -> {
            for (int i = 0; i < 10; i ++) {
                hnsw.insert(tr, createRandomPrimaryKey(random), createRandomVector(random, 728)).join();
            }
            return null;
        });
    }

    @Test
    public void testBasicInsertAndScanLayer() {
        final Random random = new Random(0);
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool());

        db.run(tr -> {
            for (int i = 0; i < 20; i ++) {
                hnsw.insert(tr, createRandomPrimaryKey(random), createRandomVector(random, 728)).join();
            }
            return null;
        });

        hnsw.scanLayer(db, 0, 100, node -> {
            System.out.println(node);
        });
    }

    private <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageAdapter<N> storageAdapter,
                                                     @Nonnull final Node<N> node,
                                                     final int layer) {
        final NeighborsChangeSet<N> insertChangeSet =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                        node.getNeighbors());
        storageAdapter.writeNode(transaction, node, layer, insertChangeSet);
    }

    @Nonnull
    private Node<NodeReference> createRandomCompactNode(@Nonnull final Random random,
                                                        @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                                        final int dimensionality,
                                                        final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReference> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReference(random));
        }

        return nodeFactory.create(primaryKey, createRandomVector(random, dimensionality), neighborsBuilder.build());
    }

    @Nonnull
    private Node<NodeReferenceWithVector> createRandomInliningNode(@Nonnull final Random random,
                                                                   @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                                                   final int dimensionality,
                                                                   final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReferenceWithVector(random, dimensionality));
        }

        return nodeFactory.create(primaryKey, createRandomVector(random, dimensionality), neighborsBuilder.build());
    }

    @Nonnull
    private NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(createRandomPrimaryKey(random));
    }

    @Nonnull
    private NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random, final int dimensionality) {
        return new NodeReferenceWithVector(createRandomPrimaryKey(random), createRandomVector(random, dimensionality));
    }

    @Nonnull
    private static Tuple createRandomPrimaryKey(final @Nonnull Random random) {
        return Tuple.from(random.nextLong());
    }

    @Nonnull
    private Vector.HalfVector createRandomVector(@Nonnull final Random random, final int dimensionality) {
        final Half[] components = new Half[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            // don't ask
            components[d] = HNSWHelpers.halfValueOf(random.nextDouble());
        }
        return new Vector.HalfVector(components);
    }
}

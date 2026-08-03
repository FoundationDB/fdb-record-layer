/*
 * CardinalityTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnReadListener;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnWriteListener;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HNSW#cardinality(com.apple.foundationdb.ReadTransaction)}, which classifies layer 0 of an
 * {@link HNSW} as {@link Cardinality#EMPTY}, {@link Cardinality#SINGLE} or {@link Cardinality#MULTIPLE}. Layer 0
 * holds every node, so the cases correspond to a graph that is empty, holds exactly one node, or holds two or
 * more nodes.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
class CardinalityTest implements BaseTest {
    private static final int NUM_DIMENSIONS = 128;
    private static final long SEED = 0x10ADC0DEL;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private Database db;

    @BeforeEach
    public void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @Nonnull
    @Override
    public Database getDb() {
        return db;
    }

    @Nonnull
    @Override
    public Subspace getSubspace() {
        return subspaceExtension.getSubspace();
    }

    @Nonnull
    @Override
    public Path getTempDir() {
        return tempDir;
    }

    @Test
    void emptyGraphIsEmpty() {
        final HNSW hnsw = newHnsw();
        assertThat(cardinality(hnsw)).isEqualTo(Cardinality.EMPTY);
    }

    @Test
    void graphWithOneNodeIsSingle() throws Exception {
        final HNSW hnsw = newHnsw();
        insert(hnsw, 1);
        assertThat(cardinality(hnsw)).isEqualTo(Cardinality.SINGLE);
    }

    @Test
    void graphWithTwoNodesIsMultiple() throws Exception {
        final HNSW hnsw = newHnsw();
        insert(hnsw, 2);
        assertThat(cardinality(hnsw)).isEqualTo(Cardinality.MULTIPLE);
    }

    @Test
    void graphWithManyNodesIsMultiple() throws Exception {
        final HNSW hnsw = newHnsw();
        insert(hnsw, 25);
        assertThat(cardinality(hnsw)).isEqualTo(Cardinality.MULTIPLE);
    }

    @Nonnull
    private HNSW newHnsw() {
        return new HNSW(getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().build(NUM_DIMENSIONS),
                new TestOnWriteListener(), new TestOnReadListener());
    }

    @Nonnull
    private Cardinality cardinality(@Nonnull final HNSW hnsw) {
        return db.run(tr -> hnsw.cardinality(tr).join());
    }

    private void insert(@Nonnull final HNSW hnsw, final int count) throws Exception {
        final List<PrimaryKeyAndVector> data =
                CommonTestHelpers.randomVectors(new Random(SEED), NUM_DIMENSIONS, count);
        TestHelpers.basicInsertBatch(getDb(), hnsw, count, 0,
                (tr, nextId) -> data.get(Math.toIntExact(nextId)));
    }
}

/*
 * RankedSetTest.java
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RankedSet}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class RankedSetTest {
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rsSubspaceExtension = new TestSubspaceExtension(dbExtension);
    private Database db;
    private Subspace rsSubspace;

    private RankedSet.Config config = RankedSet.DEFAULT_CONFIG;
    private static final boolean TRACE = false;

    @BeforeEach
    public void setUp() throws Exception {
        FDB fdb = FDB.instance();
        if (TRACE) {
            NetworkOptions options = fdb.options();
            options.setTraceEnable("/tmp");
            options.setTraceLogGroup("RankedSetTest");
        }
        this.db = dbExtension.getDatabase();
        this.rsSubspace = rsSubspaceExtension.getSubspace();
    }

    @Test
    public void basic() {
        basicOperations(RankedSet.DEFAULT_HASH_FUNCTION, RankedSet.DEFAULT_HASH_FUNCTION);
    }

    @Test
    public void basicCrc() {
        basicOperations(RankedSet.CRC_HASH, RankedSet.CRC_HASH);
    }

    @Test
    public void basicChange() {
        basicOperations(RankedSet.JDK_ARRAY_HASH, RankedSet.CRC_HASH);
    }

    @Test
    public void basicRandom() {
        basicOperations(RankedSet.RANDOM_HASH, RankedSet.RANDOM_HASH);
    }

    private void basicOperations(RankedSet.HashFunction firstHashFunction, RankedSet.HashFunction secondHashFunction) {
        byte[][] keys = new byte[100][];
        for (int i = 0; i < 100; ++i) {
            keys[i] = Tuple.from(String.valueOf((char)i)).pack();
        }
        db.run(tr -> {
            config = RankedSet.newConfigBuilder().setHashFunction(firstHashFunction).build();
            RankedSet rs = newRankedSet();
            for (byte[] k : keys) {
                boolean wasNew = rs.add(tr, k).join();
                assertTrue(wasNew);
            }
            assertFalse(rs.add(tr, keys[10]).join());
            config = RankedSet.newConfigBuilder().setHashFunction(secondHashFunction).build();
            rs = newRankedSet();
            for (int i = 0; i < keys.length; ++i) {
                long rank = rs.rank(tr, keys[i]).join();
                assertEquals(i, rank);
            }
            for (int i = 0; i < keys.length; ++i) {
                byte[] nth = rs.getNth(tr, i).join();
                assertArrayEquals(keys[i], nth);
            }
            for (byte[] k : keys) {
                boolean wasOld = rs.remove(tr, k).join();
                assertTrue(wasOld);
            }
            assertFalse(rs.remove(tr, keys[20]).join());
            return null;
        });
    }

    @Test
    public void duplicates() {
        byte[][] keys = new byte[10][];
        for (int i = 0; i < 10; ++i) {
            keys[i] = Tuple.from(i).pack();
        }
        config = RankedSet.newConfigBuilder().setCountDuplicates(true).build();
        db.run(tr -> {
            RankedSet rs = newRankedSet();
            for (int i = 0; i < keys.length; ++i) {
                for (int j = 1; j <= i + 1; j++) {
                    boolean wasNew = rs.add(tr, keys[i]).join();
                    assertTrue(wasNew);
                }
            }
            long size = rs.size(tr).join();
            assertEquals(10 * 11 / 2, size);
            for (int i = 0; i < keys.length; ++i) {
                long count = rs.count(tr, keys[i]).join();
                assertEquals(i + 1, count);
            }
            for (int i = 0, n = 0; i < keys.length; ++i, n += i) {
                long rank = rs.rank(tr, keys[i]).join();
                assertEquals(n, rank);
            }
            for (int i = 0, n = 0; i < keys.length; ++i) {
                for (int j = 1; j <= i + 1; j++) {
                    byte[] nth = rs.getNth(tr, n).join();
                    assertArrayEquals(keys[i], nth);    // Same key number of occurrences times.
                    n++;
                }
            }
            for (int i = 0; i < keys.length; ++i) {
                for (int j = 1; j <= i + 2; j++) {
                    boolean wasOld = rs.remove(tr, keys[i]).join();
                    assertEquals(j <= i + 1, wasOld);
                }
            }
            return null;
        });
    }

    @Test
    public void concurrentAdd() throws Exception {
        // 20 does go onto level 1, 30 and 40 do not. There should be no reason for them to conflict on level 0.
        RankedSet rs = newRankedSet();
        db.run(tr -> {
            rs.add(tr, Tuple.from(20).pack()).join();
            return null;
        });
        Transaction tr1 = db.createTransaction();
        if (TRACE) {
            tr1.options().setDebugTransactionIdentifier("tr1");
            tr1.options().setLogTransaction();
        }
        Transaction tr2 = db.createTransaction();
        if (TRACE) {
            tr2.options().setDebugTransactionIdentifier("tr2");
            tr2.options().setLogTransaction();
        }
        rs.add(tr1, Tuple.from(30).pack()).join();
        rs.add(tr2, Tuple.from(40).pack()).join();
        tr1.commit().join();
        tr2.commit().join();
        db.read(tr -> {
            assertEquals(0L, rs.rank(tr, Tuple.from(20).pack()).join().longValue());
            assertEquals(1L, rs.rank(tr, Tuple.from(30).pack()).join().longValue());
            assertEquals(2L, rs.rank(tr, Tuple.from(40).pack()).join().longValue());
            return null;
        });
    }

    @Test
    public void concurrentRemove() throws Exception {
        RankedSet rs = newRankedSet();
        db.run(tr -> {
            // Create a higher level entry.
            rs.add(tr, Tuple.from(20).pack()).join();
            return null;
        });
        Transaction tr1 = db.createTransaction();
        if (TRACE) {
            tr1.options().setDebugTransactionIdentifier("tr1");
            tr1.options().setLogTransaction();
        }
        Transaction tr2 = db.createTransaction();
        if (TRACE) {
            tr2.options().setDebugTransactionIdentifier("tr2");
            tr2.options().setLogTransaction();
        }
        // Will remove from all levels.
        rs.remove(tr1, Tuple.from(20).pack()).join();
        // Needs to increment the leftmost entry, not the one being removed.
        rs.add(tr2, Tuple.from(30).pack()).join();
        tr1.commit().join();
        assertThrows(CompletionException.class, () -> tr2.commit().join());
        db.run(tr -> {
            rs.add(tr, Tuple.from(30).pack()).join();
            return null;
        });
        db.read(tr -> {
            // If the overlapping commit had succeeded, it would have incremented the 20 entry at level 1, so 20 would be returned here.
            assertEquals(30, Tuple.fromBytes(rs.getNth(tr, 0).join()).getLong(0));
            return null;
        });
    }

    @Test
    @Tag(Tags.Slow)
    public void randomSingleThread() {
        thousandRankedSetOps(db, newRankedSet());
    }

    @Test
    @Tag(Tags.Slow)
    public void randomFiveThreads() throws InterruptedException {
        List<Throwable> uncaught = Collections.synchronizedList(new ArrayList<>());
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(() -> {
                thousandRankedSetOps(db, newRankedSet());
            }, "RSTest" + i);
            threads[i].setUncaughtExceptionHandler((t, e) -> uncaught.add(e));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(60 * 1000);
        }
        assertEquals("[]", uncaught.toString());
    }

    @Test
    @Tag(Tags.Slow)
    public void randomFiveThreadsWithDuplicates() throws InterruptedException {
        config = RankedSet.newConfigBuilder().setCountDuplicates(true).build();
        randomFiveThreads();
    }

    @Test
    public void rankAsThoughPresent() {
        RankedSet rs = newRankedSet();
        db.run(tr -> {
            for (int i = 5; i < 100; i += 10) {
                rs.add(tr, Tuple.from(i).pack()).join();
            }
            for (int i = 0; i < 100; i++) {
                assertEquals((i + 4) / 10, rs.rank(tr, Tuple.from(i).pack(), false).join().intValue());
            }
            return null;
        });
    }

    //
    // Helpers
    //

    private RankedSet newRankedSet() {
        RankedSet result = new RankedSet(rsSubspace, TestExecutors.defaultThreadPool(), config);
        result.init(db).join();
        return result;
    }

    private void rankedSetOp(TransactionContext tc, RankedSet rs) {
        int op = ThreadLocalRandom.current().nextInt(6);
        byte[] key = new byte[1];
        ThreadLocalRandom.current().nextBytes(key);
        tc.run(tr -> {
            switch (op) {
                case 0: {
                    rs.add(tr, key).join();
                    break;
                }
                case 1: {
                    long size1 = rs.size(tr).join();
                    boolean wasPresent = rs.contains(tr, key).join();
                    boolean didInsert = rs.add(tr, key).join();
                    long size2 = rs.size(tr).join();
                    if (config.isCountDuplicates()) {
                        assertTrue(didInsert);
                        assertEquals(size1 + 1, size2);
                    } else {
                        assertNotEquals(wasPresent, didInsert);
                        assertEquals(size1 + (wasPresent ? 0 : 1), size2);
                    }
                    break;
                }
                case 2: {
                    rs.remove(tr, key).join();
                    break;
                }
                case 3: {
                    long size1 = rs.size(tr).join();
                    boolean wasPresent = rs.contains(tr, key).join();
                    boolean didErase = rs.remove(tr, key).join();
                    long size2 = rs.size(tr).join();
                    assertEquals(wasPresent, didErase);
                    assertEquals(size1 - (wasPresent ? 1 : 0), size2);
                    break;
                }
                case 4: {
                    rs.clear(tr).join();
                    assertEquals(0, rs.size(tr).join().longValue());
                    break;
                }
                case 5:
                    long size = rs.size(tr).join();
                    if (size > 0) {
                        long r = ThreadLocalRandom.current().nextLong(size);
                        byte[] k = rs.getNth(tr, r).join();
                        long r2 = rs.rank(tr, k).join();
                        if (config.isCountDuplicates()) {
                            long d = rs.count(tr, k).join();
                            long r3 = rs.getRangeList(tr, new byte[] {0x00}, k).stream()
                                    .map(pk -> rs.count(tr, pk).join())
                                    .mapToLong(Long::longValue).sum();
                            if (!(d > 0 && r >= r2 && r < r2 + d && r2 == r3)) {
                                throw new IllegalStateException("Rank Mismatch: Key=" + ByteArrayUtil.printable(k) + "; d=" + d + "; r=" + r + "; r2=" + r2 + "; r3=" + r3);
                            }
                        } else {
                            long r3 = rs.getRangeList(tr, new byte[] {0x00}, k).size();
                            if (!(r == r2 && r2 == r3)) {
                                throw new IllegalStateException("Rank Mismatch: Key=" + ByteArrayUtil.printable(k) + "; r=" + r + "; r2=" + r2 + "; r3=" + r3);
                            }
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("op: " + op);
            }
            final RankedSet.Consistency consistency = rs.checkConsistency(tr);
            assertTrue(consistency.isConsistent(), consistency.toString());
            return null;
        });
    }

    private void thousandRankedSetOps(TransactionContext tc, RankedSet rs) {
        for (int i = 0; i < 1000; ++i) {
            rankedSetOp(tc, rs);
        }
    }
}

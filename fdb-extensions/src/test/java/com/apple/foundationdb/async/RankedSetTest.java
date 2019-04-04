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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RankedSet}.
 */
@Tag(Tags.RequiresFDB)
public class RankedSetTest
{
    private Database db;
    private Subspace rsSubspace;

    private static final boolean TRACE = false;

    @BeforeEach
    public void setUp() throws Exception {
        FDB fdb = FDB.selectAPIVersion(600);
        if (TRACE) {
            NetworkOptions options = fdb.options();
            options.setTraceEnable("/tmp");
            options.setTraceLogGroup("RankedSetTest");
        }
        this.db = fdb.open();
        this.rsSubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(getClass().getSimpleName())).get();
        db.run(tr -> {
            tr.clear(rsSubspace.range());
            return null;
        });
    }

    @Test
    public void simple() throws Exception {
        RankedSet rs = newRankedSet();
        db.run(tr -> {
            byte[][] keys = new byte[100][];
            for (int i = 0; i < 100; ++i) {
                keys[i] = Tuple.from(String.valueOf((char)i)).pack();
            }
            for (byte[] k : keys) {
                boolean wasNew = rs.add(tr, k).join();
                assertTrue(wasNew);
            }
            for (int i = 0; i < keys.length; ++i) {
                long rank = rs.rank(tr, keys[i]).join();
                assertEquals(i, rank);
            }
            for (int i = 0; i < keys.length; ++i) {
                byte[] nth = rs.getNth(tr, i).join();
                assertArrayEquals(keys[i], nth);
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
            tr1.options().setTransactionLoggingEnable("tr1");
        }
        Transaction tr2 = db.createTransaction();
        if (TRACE) {
            tr2.options().setTransactionLoggingEnable("tr2");
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
            tr1.options().setTransactionLoggingEnable("tr1");
        }
        Transaction tr2 = db.createTransaction();
        if (TRACE) {
            tr2.options().setTransactionLoggingEnable("tr2");
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
        RankedSet result = new RankedSet(rsSubspace, ForkJoinPool.commonPool());
        result.init(db).join();
        return result;
    }

    private static void rankedSetOp(TransactionContext tc, RankedSet rs) {
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
                    assertNotEquals(wasPresent, didInsert);
                    assertEquals(size1 + (wasPresent ? 0 : 1), size2);
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
                        long r3 = rs.getRangeList(tr, new byte[]{ 0x00 }, k).size();
                        if (!(r == r2 && r2 == r3)) {
                            throw new IllegalStateException(String.format("Rank Mismatch: Key=%s; r=%d; r2=%d; r3=%d",
                                                                          ByteArrayUtil.printable(k),
                                                                          r,
                                                                          r2,
                                                                          r3));
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

    private static void thousandRankedSetOps(TransactionContext tc, RankedSet rs) {
        for (int i = 0; i < 1000; ++i) {
            rankedSetOp(tc, rs);
        }
    }
}

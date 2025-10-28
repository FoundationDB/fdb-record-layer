/*
 * ConcatCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.TriFunction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ConcatCursor}.
 */
@Tag(Tags.RequiresFDB)
public class ConcatCursorTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    FDBDatabase fdb;
    FDBRecordContext context;
    private Subspace subspace;
    ExecuteProperties ep;

    @BeforeEach
    public void setUp() throws Exception {
        fdb = dbExtension.getDatabase();
        context = fdb.openContext();
        setupBaseData();
    }

    @AfterEach
    public void tearDown() throws Exception {
        context.close();
    }

    @Test
    public void simpleForwardScanTest() {
        concatListCursorTests(ScanProperties.FORWARD_SCAN);
    }

    @Test
    public void simpleBackwardScanTest() {
        concatListCursorTests(ScanProperties.REVERSE_SCAN);
    }

    @Test
    public void simpleRowLimitTest() {
        int rowLimit;

        //Row limit tests ...
        ScanProperties scanProperties;
        int maxRows = 51;
        for (rowLimit = 0; rowLimit < maxRows; rowLimit++) {
            ep = ExecuteProperties.newBuilder().setReturnedRowLimit(rowLimit).build();
            scanProperties = new ScanProperties(ep);
            if (rowLimit % 2 == 0) {
                scanProperties.setReverse(true); //toggle between forward and reverse scans
            } else {
                scanProperties.setReverse(false);
            }
            concatRowLimitTest(scanProperties);
        }
    }

    @Tag(Tags.Slow)
    @Test
    public void simpleTimeLimitTest() {

        ScanProperties scanProperties;
        //Time limited tests
        long maxTimeLimit = 1000;
        long timeLimit;
        for (timeLimit = 1; timeLimit < maxTimeLimit; timeLimit = timeLimit * 10) {
            ep = ExecuteProperties.newBuilder().setTimeLimit(timeLimit).build();
            scanProperties = new ScanProperties(ep);
            concatTimeLimitTest(scanProperties, timeLimit);
        }
    }

    public void concatListCursorTests(ScanProperties scanProperties) {

        //create some list cursors
        List<Integer> ints = Arrays.asList(1, 2, 3);
        List<Integer> ints2 = Arrays.asList(4, 5, 6);

        List<Integer> resultTwoCursors = new ArrayList<>();
        resultTwoCursors.addAll(ints);
        resultTwoCursors.addAll(ints2);
        if (scanProperties.isReverse()) {
            Collections.reverse(resultTwoCursors);
        }

        List<Integer> ints3 = Arrays.asList(7, 8, 9);
        List<Integer> resultThreeCursors = new ArrayList<>();
        resultThreeCursors.addAll(ints);
        resultThreeCursors.addAll(ints2);
        resultThreeCursors.addAll(ints3);
        if (scanProperties.isReverse()) {
            Collections.reverse(resultThreeCursors);
        }

        List<Integer> ints4 = Arrays.asList(10, 11, 12);
        List<Integer> resultFourCursors = new ArrayList<>();
        resultFourCursors.addAll(ints);
        resultFourCursors.addAll(ints2);
        resultFourCursors.addAll(ints3);
        resultFourCursors.addAll(ints4);
        if (scanProperties.isReverse()) {
            Collections.reverse(resultFourCursors);
        }

        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> lc1 = (c, s, b) -> {
            List<Integer> l = (ints.stream().collect(toList()));
            if (s.isReverse()) {
                Collections.reverse(l);
            }
            return RecordCursor.fromList(l, b);
        };
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> lc2 = (c, s, b) -> {
            List<Integer> l = (ints2.stream().collect(toList()));
            if (s.isReverse()) {
                Collections.reverse(l);
            }
            return RecordCursor.fromList(l, b);
        };

        ConcatCursor<Integer> cc;
        TestResult testResult;

        cc = new ConcatCursor<>(context, scanProperties, lc1, lc2, null);
        testResult = iterateAndCompare(resultTwoCursors, cc, resultTwoCursors.size());

        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> fcc1;

        //concat three list cursors with the bushy tree first
        fcc1 = (c, s, b) -> new ConcatCursor<>(c, s, lc1, lc2, b);
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> lc3 = (c, s, b) -> {
            List<Integer> l = (ints3.stream().collect(toList()));
            if (s.isReverse()) {
                Collections.reverse(l);
            }
            return RecordCursor.fromList(l, b);
        };

        cc = new ConcatCursor<>(context, scanProperties, fcc1, lc3, null);
        testResult = iterateAndCompare(resultThreeCursors, cc, resultThreeCursors.size());

        //concat three list cursors with a bushy second
        fcc1 = (c, s, b) -> new ConcatCursor<>(c, s, lc2, lc3, b);
        cc = new ConcatCursor<>(context, scanProperties, lc1, fcc1, null);
        testResult = iterateAndCompare(resultThreeCursors, cc, resultThreeCursors.size());

        //concat four list cursors with a bushy tree
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> lc4 = (c, s, b) -> {
            List<Integer> l = (ints4.stream().collect(toList()));
            if (s.isReverse()) {
                Collections.reverse(l);
            }
            return RecordCursor.fromList(l, b);
        };
        fcc1 = (c, s, b) -> new ConcatCursor<>(c, s, lc1, lc2, b);

        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<Integer>> fcc2;
        fcc2 = (c, s, b) -> new ConcatCursor<>(c, s, lc3, lc4, b);
        cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, null);
        testResult = iterateAndCompare(resultFourCursors, cc, resultFourCursors.size());

        //stop and restart the cursor at each each possible location in the sequence
        Integer stop;
        for (stop = 1; stop < resultFourCursors.size(); stop++) {
            cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, null);
            testResult = iterateAndCompare(resultFourCursors, cc, stop);
            cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, testResult.getContinuation());
            testResult = iterateAndCompare(resultFourCursors.subList(stop, resultFourCursors.size()), cc, resultFourCursors.size());
        }
    }

    public void concatRowLimitTest(ScanProperties scanProperties) {

        RecordCursor<KeyValue> cc;
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<KeyValue>> fcc1 = (c, s, b) -> KeyValueCursor.Builder.withSubspace(subspace).setScanProperties(s).setContext(c)
                .setRange(TupleRange.ALL).setContinuation(b).build();
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<KeyValue>> fcc2 = (c, s, b) -> KeyValueCursor.Builder.withSubspace(subspace).setScanProperties(s).setContext(c)
                .setRange(TupleRange.ALL).setContinuation(b).build();

        Integer firstCursorSize = fcc1.apply(context, ScanProperties.FORWARD_SCAN, null).getCount().join();
        Integer secondCursorSize = fcc1.apply(context, ScanProperties.FORWARD_SCAN, null).getCount().join();
        Integer concatCursorSize = firstCursorSize + secondCursorSize;
        Integer expectedResultSize;
        Integer rowLimit = scanProperties.getExecuteProperties().getReturnedRowLimit();
        if (rowLimit > 0 && rowLimit <= concatCursorSize) {
            expectedResultSize = Integer.min(rowLimit, concatCursorSize);
        } else {
            expectedResultSize = concatCursorSize; // no row limit
        }

        //concat a couple of kv cursors and check that we get expected number of rows
        cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, null);
        Assertions.assertEquals(expectedResultSize, (Integer)cc.asList().join().size());

        //now run the same tests breaking up the concat cursor runs and use continuation to complete
        //stop and restart the cursor at each each possible location in the sequence
        Integer stop;
        TestResult testResult1;
        TestResult testResult2;
        Integer totalRowsScanned;
        for (stop = 1; stop < concatCursorSize; stop++) {
            cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, null);
            testResult1 = iterateAndCompare(null, cc, stop);
            cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, testResult1.getContinuation());
            testResult2 = iterateAndCompare(null, cc, expectedResultSize);
            totalRowsScanned = testResult1.getCount() + testResult2.getCount();
            Assertions.assertEquals(totalRowsScanned, (Integer)(rowLimit == 0 ? concatCursorSize : Integer.min((Integer.min(stop, rowLimit) + rowLimit), concatCursorSize)));  //each invocation can only get as far as rowLimit
        }
    }

    public void concatTimeLimitTest(ScanProperties scanProperties, long delay) {

        RecordCursor<KeyValue> cc;
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<KeyValue>> fcc1 = (c, s, b) -> KeyValueCursor.Builder.withSubspace(subspace).setScanProperties(s).setContext(c)
                .setRange(TupleRange.ALL).setContinuation(b).build().map(v -> {
                    try {
                        sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return v;
                });
        TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<KeyValue>> fcc2 = (c, s, b) -> KeyValueCursor.Builder.withSubspace(subspace).setScanProperties(s).setContext(c)
                .setRange(TupleRange.ALL).setContinuation(b).build().map(v -> {
                    try {
                        sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return v;
                });

        Integer firstCursorSize = fcc1.apply(context, ScanProperties.FORWARD_SCAN, null).getCount().join();
        Integer secondCursorSize = fcc1.apply(context, ScanProperties.FORWARD_SCAN, null).getCount().join();
        Integer concatCursorSize = firstCursorSize + secondCursorSize;
        Integer recordCount = 0;
        TestResult testResult;
        byte[] continuation = null;
        while (recordCount < concatCursorSize) {
            cc = new ConcatCursor<>(context, scanProperties, fcc1, fcc2, continuation);
            testResult = iterateAndCompare(null, cc, concatCursorSize);
            recordCount += testResult.getCount();
            continuation = testResult.getContinuation();
            if (continuation == null) {
                break;
            }
        }
        Assertions.assertEquals(recordCount, concatCursorSize);
    }

    private TestResult iterateAndCompare(List<?> result, RecordCursor<?> cc, Integer limit) {
        Integer i = 0;
        byte[] next = null;
        RecordCursorResult<?> nr;
        Boolean hasNext = true;
        Integer count = 0;
        Object rr;

        while (i < limit && hasNext) {
            try {
                nr = cc.onNext().get();
                if ((hasNext = nr.hasNext()) == true) {
                    rr = nr.get();
                    if (result != null) {
                        assertEquals(result.get(i), rr);
                    }
                    next = nr.getContinuation().toBytes();
                    count++;
                }
            } catch (Exception e) {
                Assertions.fail("onNext() future completed exceptionally.");
            }
            i++;
        }
        return new TestResult(next, count);
    }

    private class TestResult {
        private final byte[] continuation;
        private final Integer count;

        public TestResult(byte[] continuation, Integer count) {
            this.continuation = continuation;
            this.count = count;
        }

        private byte[] getContinuation() {
            return this.continuation;
        }

        private Integer getCount() {
            return this.count;
        }
    }

    private void setupBaseData() {
        subspace = fdb.run(context -> {
            KeySpacePath path = pathManager.createPath();
            return path.toSubspace(context);
        });

        // Populate with data.
        fdb.database().run(tr -> {
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) {
                    tr.set(subspace.pack(Tuple.from(i, j)), Tuple.from(i, j).pack());
                }
            }
            return null;
        });
    }

    //TODO test with concat cursor
}

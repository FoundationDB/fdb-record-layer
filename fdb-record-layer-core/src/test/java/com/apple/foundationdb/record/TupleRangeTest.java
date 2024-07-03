/*
 * TupleRangeTest.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link TupleRange}.
 */
public class TupleRangeTest {
    private static final Tuple PREFIX_TUPLE = Tuple.from("prefix");
    private static final Subspace PREFIX_SUBSPACE = new Subspace(PREFIX_TUPLE);
    private static final byte[] PREFIX_BYTES = PREFIX_SUBSPACE.getKey();

    @Test
    public void toRange() {
        final Tuple a = Tuple.from("a");
        final Tuple b = Tuple.from("b");
        List<NonnullPair<TupleRange, Range>> testCases = Arrays.asList(
                NonnullPair.of(TupleRange.allOf(null), new Range(new byte[0], new byte[]{(byte)0xff})),
                NonnullPair.of(TupleRange.allOf(PREFIX_TUPLE), new Range(PREFIX_BYTES, ByteArrayUtil.join(PREFIX_BYTES, new byte[] { (byte)0xff }))),

                NonnullPair.of(new TupleRange(a, b, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                        new Range(a.pack(), b.pack())),
                NonnullPair.of(new TupleRange(a, b, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE),
                        new Range(a.pack(), ByteArrayUtil.join(b.pack(), new byte[] { (byte)0xff }))),
                NonnullPair.of(new TupleRange(a, b, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                        new Range(ByteArrayUtil.strinc(a.pack()), b.pack())),
                NonnullPair.of(new TupleRange(a, b, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_INCLUSIVE),
                        new Range(ByteArrayUtil.strinc(a.pack()), ByteArrayUtil.join(b.pack(), new byte[] { (byte)0xff }))),
                NonnullPair.of(new TupleRange(null, b, EndpointType.TREE_START, EndpointType.RANGE_EXCLUSIVE),
                        new Range(new byte[0], b.pack())),
                NonnullPair.of(new TupleRange(a, null, EndpointType.RANGE_INCLUSIVE, EndpointType.TREE_END),
                        new Range(a.pack(), new byte[]{(byte)0xff})),

                NonnullPair.of(new TupleRange(a, b, EndpointType.CONTINUATION, EndpointType.RANGE_EXCLUSIVE),
                        new Range(ByteArrayUtil.join(a.pack(), new byte[]{0x00}), b.pack())),
                NonnullPair.of(new TupleRange(a, b, EndpointType.RANGE_INCLUSIVE, EndpointType.CONTINUATION),
                        new Range(a.pack(), b.pack())),

                NonnullPair.of(TupleRange.prefixedBy("a"),
                        new Range(new byte[]{0x02, (byte)'a'}, new byte[]{0x02, (byte)'b'})),
                NonnullPair.of(new TupleRange(Tuple.from("apple"), a, EndpointType.CONTINUATION, EndpointType.PREFIX_STRING),
                        new Range(ByteArrayUtil.join(Tuple.from("apple").pack(), new byte[]{0x00}), new byte[]{0x02, (byte)'b'})),
                NonnullPair.of(new TupleRange(a, Tuple.from("apple"), EndpointType.PREFIX_STRING, EndpointType.CONTINUATION),
                        new Range(new byte[]{0x02, (byte)'a'}, Tuple.from("apple").pack()))
        );
        testCases.forEach(pair -> {
            assertEquals(pair.getRight(), pair.getLeft().toRange());
            TupleRange expectedRange = pair.getLeft().prepend(PREFIX_TUPLE);
            assertEquals(expectedRange.toRange(), pair.getLeft().toRange(PREFIX_SUBSPACE));
        });
    }

    @Test
    public void illegalRanges() {
        final Tuple a = Tuple.from("a");
        final Tuple b = Tuple.from("b");
        List<TupleRange> testRanges = Arrays.asList(
                new TupleRange(a, b, EndpointType.RANGE_INCLUSIVE, EndpointType.TREE_START),
                new TupleRange(a, b, EndpointType.TREE_END, EndpointType.RANGE_EXCLUSIVE),
                new TupleRange(null, b, EndpointType.RANGE_EXCLUSIVE, EndpointType.RANGE_EXCLUSIVE),
                new TupleRange(a, null, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE),

                new TupleRange(null, a, EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING),
                new TupleRange(a, b, EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING),
                new TupleRange(a, a, EndpointType.RANGE_INCLUSIVE, EndpointType.PREFIX_STRING),
                new TupleRange(a, a, EndpointType.PREFIX_STRING, EndpointType.RANGE_INCLUSIVE),
                new TupleRange(a, b, EndpointType.CONTINUATION, EndpointType.PREFIX_STRING),
                new TupleRange(a, b, EndpointType.PREFIX_STRING, EndpointType.CONTINUATION),
                new TupleRange(Tuple.from(1066), Tuple.from(1066), EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING)
        );
        testRanges.forEach(range ->
                assertThrows(RecordCoreException.class, range::toRange, () -> "range " + range + " should have thrown an error")
        );
    }
}

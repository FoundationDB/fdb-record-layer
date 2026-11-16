/*
 * RTreeIndexHelperContinuationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the R-tree scan continuation directly. Scans wrap it in an outer continuation that serializes through
 * {@code toByteString()}, so {@code toBytes()} is not reached by scan tests.
 */
class RTreeIndexHelperContinuationTest {
    @Test
    void toBytesRoundTripTest() {
        final BigInteger hilbertValue = new BigInteger("123456789012345678901234567890");
        final Tuple key = Tuple.from("A", 42L);
        final RTreeIndexHelper.Continuation continuation = new RTreeIndexHelper.Continuation(hilbertValue, key);
        assertThat(continuation.isEnd()).isFalse();

        final byte[] bytes = continuation.toBytes();
        assertThat(bytes).isNotNull().isNotEmpty();
        // Cached, and consistent with the byte string form.
        assertThat(continuation.toBytes()).isSameAs(bytes);
        assertThat(ByteString.copyFrom(bytes)).isEqualTo(continuation.toByteString());

        final RTreeIndexHelper.Continuation restored = RTreeIndexHelper.Continuation.fromBytes(bytes);
        assertThat(restored).isNotNull();
        assertThat(restored.getLastHilbertValue()).isEqualTo(hilbertValue);
        assertThat((Object)restored.getLastKey()).isEqualTo(key);
    }

    @Test
    void endContinuationTest() {
        for (final RTreeIndexHelper.Continuation end : new RTreeIndexHelper.Continuation[] {
                new RTreeIndexHelper.Continuation(null, null),
                new RTreeIndexHelper.Continuation(BigInteger.ONE, null),
                new RTreeIndexHelper.Continuation(null, Tuple.from(1L))}) {
            assertThat(end.isEnd()).isTrue();
            assertThat(end.toBytes()).isNull();
            assertThat(end.toByteString()).isEqualTo(ByteString.EMPTY);
        }
    }

    @Test
    void fromBytesTest() {
        assertThat(RTreeIndexHelper.Continuation.fromBytes(null)).isNull();
        assertThatThrownBy(() -> RTreeIndexHelper.Continuation.fromBytes(new byte[] {1, 2, 3}))
                .isInstanceOf(RecordCoreException.class);
    }
}

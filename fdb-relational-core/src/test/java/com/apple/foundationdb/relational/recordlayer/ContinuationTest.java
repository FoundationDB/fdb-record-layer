/*
 * ContinuationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.continuation.ContinuationProto;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class ContinuationTest {
    @Test
    public void testBegin() {
        ContinuationImpl continuation = ContinuationImpl.BEGIN;
        assertContinuation(continuation, true, false, null);
    }

    @Test
    public void testEnd() {
        ContinuationImpl continuation = ContinuationImpl.END;
        assertContinuation(continuation, false, true, new byte[0]);
    }

    @Test
    public void testBytes() {
        ContinuationImpl continuation = (ContinuationImpl) ContinuationImpl.fromUnderlyingBytes("Hello".getBytes());
        assertContinuation(continuation, false, false, "Hello".getBytes());
    }

    @Test
    public void testInt() {
        ContinuationImpl continuation = (ContinuationImpl) ContinuationImpl.fromInt(5);
        assertContinuation(continuation, false, false, Ints.toByteArray(5));
    }

    @Test
    public void serializeAndRestore() throws Exception {
        ContinuationImpl continuation = (ContinuationImpl) ContinuationImpl.fromUnderlyingBytes("Hello".getBytes());
        byte[] bytes = continuation.serialize();
        continuation = ContinuationImpl.parseContinuation(bytes);
        assertContinuation(continuation, false, false, "Hello".getBytes());
    }

    @Test
    public void customProto() throws Exception {
        ContinuationProto proto = ContinuationProto.newBuilder()
                .setVersion(5)
                .setExecutionState(ByteString.copyFrom("Blah".getBytes()))
                .setBindingHash(1234)
                .build();
        ContinuationImpl continuation = ContinuationImpl.parseContinuation(proto.toByteArray());
        Assertions.assertThat(continuation.atBeginning()).isEqualTo(false);
        Assertions.assertThat(continuation.atEnd()).isEqualTo(false);
        Assertions.assertThat(continuation.getExecutionState()).isEqualTo("Blah".getBytes());
        Assertions.assertThat(continuation.getVersion()).isEqualTo(5);
        Assertions.assertThat(continuation.getBindingHash()).isEqualTo(1234);
    }

    @Test
    public void testNullSameAsBegin() {
        ContinuationImpl continuation = (ContinuationImpl) ContinuationImpl.fromUnderlyingBytes(null);
        assertContinuation(continuation, true, false, null);
    }

    @Test
    public void invalidProtoFails() {
        Assertions.assertThatThrownBy(() -> ContinuationImpl.parseContinuation("Invalid".getBytes())).isInstanceOf(InvalidProtocolBufferException.class);
    }

    private void assertContinuation(ContinuationImpl continuation, boolean atBeginning, boolean atEnd, Object underlying) {
        Assertions.assertThat(continuation.atBeginning()).isEqualTo(atBeginning);
        Assertions.assertThat(continuation.atEnd()).isEqualTo(atEnd);
        Assertions.assertThat(continuation.getExecutionState()).isEqualTo(underlying);
        Assertions.assertThat(continuation.getVersion()).isEqualTo(ContinuationImpl.CURRENT_VERSION);
    }

    @Test
    // This test is here because -api doesn't depend on -core, so Options has no way of making one.
    // Note that for the same reason it is not possible to get the continuation back from the string property.
    public void testContinuationOption() throws Exception {
        final byte[] asBytes = { (byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xC1 };
        final ContinuationImpl continuation = (ContinuationImpl) ContinuationImpl.fromUnderlyingBytes(asBytes);
        final Options options = Options.builder().withOption(Options.Name.CONTINUATION, continuation).build();
        final Properties properties = Options.toProperties(options);
        // Field 1 (version): varint 1; field 2 (execution_state): len 4
        Assertions.assertThat(properties).hasFieldOrPropertyWithValue(Options.Name.CONTINUATION.name(), "08011204FEEDBAC1");
        Assertions.assertThatThrownBy(() -> Options.fromProperties(properties)).isInstanceOf(UnsupportedOperationException.class);
    }
}

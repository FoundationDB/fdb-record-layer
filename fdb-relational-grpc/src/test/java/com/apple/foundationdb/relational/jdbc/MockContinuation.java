/*
 * MockContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Continuation;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A testing implementation of a continuation.
 */
public class MockContinuation implements Continuation {
    public static final MockContinuation BEGIN = new MockContinuation(null, null, true, false);
    public static final MockContinuation END = new MockContinuation(null, new byte[0], false, true);

    private final Reason reason;
    private final byte[] executionState;
    private final boolean atBeginning;
    private final boolean atEnd;

    public MockContinuation(Reason reason, byte[] executionState, boolean atBeginning, boolean atEnd) {
        this.reason = reason;
        this.executionState = executionState;
        this.atBeginning = atBeginning;
        this.atEnd = atEnd;
    }

    @Override
    public Reason getReason() {
        return reason;
    }

    @Nullable
    @Override
    public byte[] getExecutionState() {
        return executionState;
    }

    @Override
    public boolean atBeginning() {
        return atBeginning;
    }

    @Override
    public boolean atEnd() {
        return atEnd;
    }

    @Override
    public byte[] serialize() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "MockContinuation{" +
                "reason=" + reason +
                ", executionState=" + Arrays.toString(executionState) +
                ", atBeginning=" + atBeginning +
                ", atEnd=" + atEnd +
                '}';
    }
}

/*
 * RecordCursorEndContinuation.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nullable;

/**
 * A concrete continuation representing that a {@link RecordCursor} has returned all of the records that it ever will.
 * This is the structured continuation equivalent of a {@code null} byte array continuation.
 */
@API(API.Status.INTERNAL)
public class RecordCursorEndContinuation implements RecordCursorContinuation {
    public static final RecordCursorContinuation END = new RecordCursorEndContinuation();

    private RecordCursorEndContinuation() {
    }

    @Nullable
    @Override
    public byte[] toBytes() {
        return null;
    }

    @Override
    public boolean isEnd() {
        return true;
    }
}

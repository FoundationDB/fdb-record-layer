/*
 * RecordCursorStartContinuation.java
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
 * A continuation representing the start of a cursor's execution.
 *
 * This continuation is not perfectly well behaved, for historical reasons. Specifically, its binary serialization is
 * {@code null}, which is the same as the binary serialization of {@link RecordCursorEndContinuation}. As a result,
 * the two are distinguishable only through the result of {@link #isEnd()}. Because a start continuation is only used
 * internally and a continuation can be obtained only with a {@link RecordCursorResult}---at which point the cursor is
 * no longer at its "start"---there is no ambiguity to an external API consumer. However, it remains a potential pitfall.
 */
@API(API.Status.INTERNAL)
public class RecordCursorStartContinuation implements RecordCursorContinuation {
    public static final RecordCursorContinuation START = new RecordCursorStartContinuation();

    private RecordCursorStartContinuation() {
    }

    @Override
    public boolean isEnd() {
        return false;
    }

    @Nullable
    @Override
    public byte[] toBytes() {
        return null;
    }
}

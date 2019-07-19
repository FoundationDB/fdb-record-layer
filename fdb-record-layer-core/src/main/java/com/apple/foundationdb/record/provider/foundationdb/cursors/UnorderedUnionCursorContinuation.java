/*
 * UnorderedUnionCursorContinuation.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorStartContinuation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

class UnorderedUnionCursorContinuation extends UnionCursorContinuation {
    private final int currentChild;

    private UnorderedUnionCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations,
                                             @Nullable RecordCursorProto.UnionContinuation originalProto,
                                             int currentChild) {
        super(continuations, originalProto);
        this.currentChild = currentChild;
    }

    private UnorderedUnionCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations,
                                             int currentChild) {
        this(continuations, null, currentChild);
    }

    @Override
    @Nonnull
    RecordCursorProto.UnionContinuation.Builder newProtoBuilder() {
        return RecordCursorProto.UnionContinuation.newBuilder()
                .setCurrentChild(currentChild);
    }

    int getCurrentChild() {
        return currentChild;
    }

    @Nonnull
    static UnorderedUnionCursorContinuation from(@Nonnull UnorderedUnionCursor<?> cursor) {
        return new UnorderedUnionCursorContinuation(cursor.getChildContinuations(), cursor.getCurrentChildPos());
    }

    @Nonnull
    static UnorderedUnionCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
        if (bytes == null) {
            return new UnorderedUnionCursorContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START), 0);
        }
        return UnorderedUnionCursorContinuation.from(parseProto(bytes), numberOfChildren);
    }

    @Nonnull
    static UnorderedUnionCursorContinuation from(@Nonnull RecordCursorProto.UnionContinuation parsed, int numberOfChildren) {
        return new UnorderedUnionCursorContinuation(getChildContinuations(parsed, numberOfChildren), parsed, parsed.getCurrentChild());
    }
}

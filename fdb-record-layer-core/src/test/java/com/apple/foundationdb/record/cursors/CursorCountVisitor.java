/*
 * CursorCountVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;

/**
 * A visitor that counts the total number of cursors in a cursor hierarchy.
 *
 * <p>This visitor traverses a cursor tree structure and counts each cursor it encounters,
 * including nested cursors. It implements the visitor pattern to walk through composite
 * cursor structures like {@link RecursiveCursor} and other cursor types that contain
 * child cursors.
 *
 * <p>The count includes the root cursor and all nested cursors at any depth.
 */
class CursorCountVisitor implements RecordCursorVisitor {
    private int cursorCount = 0;

    @Override
    public boolean visitEnter(RecordCursor<?> cursor) {
        cursorCount++;
        return true;
    }

    @Override
    public boolean visitLeave(RecordCursor<?> cursor) {
        return true;
    }

    /**
     * Counts the total number of cursors in the given cursor hierarchy.
     *
     * <p>This method creates a visitor and traverses the cursor tree to count all cursors,
     * including the root cursor and any nested child cursors at any depth.
     *
     * @param cursor the root cursor to count cursors for
     * @return the total number of cursors in the hierarchy
     */
    public static int getCursorsCount(@Nonnull final RecordCursor<?> cursor) {
        final var visitor = new CursorCountVisitor();
        cursor.accept(visitor);
        return visitor.cursorCount;
    }
}

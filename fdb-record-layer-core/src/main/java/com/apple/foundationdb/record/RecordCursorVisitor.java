/*
 * RecordCursorVisitor.java
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

/**
 * A hierarchical visitor for record cursor trees designed mostly to allow tests to gather information without adding
 * invasive query methods to the {@link RecordCursor} interface.
 * Note that the "ordering" of children of each <code>RecordCursor</code> is determined by individual implementors.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordCursorVisitor {
    /**
     * Called on nodes in the record cursor tree in visit pre-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the first time, <code>visitEnter()</code> is called on that node.
     * @param cursor the cursor to visit
     * @return <code>true</code> if the children of <code>cursor</code> should be visited, and <code>false</code> if they should not be visited
     */
    boolean visitEnter(RecordCursor<?> cursor);

    /**
     * Called on nodes in the record cursor tree in visit post-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the last time (after all of its children have been visited, if applicable),
     * <code>visitLeave()</code> is called on that node.
     * @param cursor the cursor to visit
     * @return <code>true</code> if the subsequent siblings of the <code>cursor</code> should be visited, and <code>false</code> otherwise
     */
    boolean visitLeave(RecordCursor<?> cursor);
}

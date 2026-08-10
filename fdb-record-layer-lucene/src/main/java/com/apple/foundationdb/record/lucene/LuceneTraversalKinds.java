/*
 * LuceneTraversalKinds.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.IndexTraversalKind;

/**
 * {@link IndexTraversalKind}s for Lucene.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneTraversalKinds {
    /**
     * A Lucene index, held in a directory of its own rather than as the plain key-value pairs every traversal the core
     * defines walks. Which of the Lucene scan types is in play does not change that, so they all report this one.
     */
    public static final IndexTraversalKind LUCENE = new IndexTraversalKind("LUCENE");

    private LuceneTraversalKinds() {
    }
}

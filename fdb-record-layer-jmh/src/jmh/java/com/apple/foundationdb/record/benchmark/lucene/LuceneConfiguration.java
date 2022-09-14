/*
 * LuceneConfiguration.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark.lucene;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class LuceneConfiguration {
    @Param("201")
    public int lucenePageSize;

    @Param("10")
    public int numMailboxes;

    @Param("0")
    public int mailboxOffset;

    @Param("philodendron monstera")
    public String queryTerm;

    //when set to -1, that means don't limit the thread-count
    @Param("-1")
    public int numLuceneThreads;

    @Param("-1")
    public int numContextThreads;

    @Param("10000")
    public int numRecords;
}

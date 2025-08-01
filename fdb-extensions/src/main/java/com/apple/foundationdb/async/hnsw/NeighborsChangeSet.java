/*
 * InliningNode.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * TODO.
 */
interface NeighborsChangeSet<N extends NodeReference> {
    @Nullable
    NeighborsChangeSet<N> getParent();

    @Nonnull
    Iterable<N> merge();

    void writeDelta(@Nonnull InliningStorageAdapter storageAdapter, @Nonnull Transaction transaction, int layer,
                    @Nonnull Node<N> node, @Nonnull Predicate<Tuple /* primary key */> primaryKeyPredicate);
}

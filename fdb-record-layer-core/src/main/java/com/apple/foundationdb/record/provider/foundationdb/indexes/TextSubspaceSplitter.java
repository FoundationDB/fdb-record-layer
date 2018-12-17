/*
 * TextSubspaceSplitter.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.map.SubspaceSplitter;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// A (package private) implementation of the SubspaceSplitter interface that is
// used by the text index. This will group the subspace by the number of grouping
// columns to provided the tuple to prefix elements of an index scan with.
class TextSubspaceSplitter implements SubspaceSplitter<Tuple> {
    @Nonnull private final Subspace indexSubspace;
    private final int groupingColumns;

    public TextSubspaceSplitter(@Nonnull Subspace indexSubspace, int groupingColumns) {
        this.indexSubspace = indexSubspace;
        this.groupingColumns = groupingColumns;
    }

    @Nonnull
    @Override
    public Subspace subspaceOf(@Nonnull byte[] keyBytes) {
        Tuple t = indexSubspace.unpack(keyBytes);
        return indexSubspace.subspace(TupleHelpers.subTuple(t, 0, groupingColumns));
    }

    @Nullable
    @Override
    public Tuple subspaceTag(@Nonnull Subspace subspace) {
        return indexSubspace.unpack(subspace.getKey());
    }
}

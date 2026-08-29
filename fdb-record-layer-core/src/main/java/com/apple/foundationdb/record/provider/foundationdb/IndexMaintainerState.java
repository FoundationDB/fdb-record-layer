/*
 * IndexMaintainerState.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.subspace.Subspace;


/**
 * Common state for an {@link IndexMaintainer}.
 *
 * In particular, this includes the record store and index meta-data for the maintainer.
 *
 */
@API(API.Status.UNSTABLE)
public class IndexMaintainerState {
    public final FDBRecordStore store;
    public final FDBRecordContext context;
    public final Index index;
    public final Subspace indexSubspace;
    public final Transaction transaction;
    public final IndexMaintenanceFilter filter;

    public IndexMaintainerState(FDBRecordStore store, FDBRecordContext context,
                                Index index, Subspace indexSubspace, Transaction transaction,
                                IndexMaintenanceFilter filter) {
        this.store = store;
        this.context = context;
        this.index = index;
        this.indexSubspace = indexSubspace;
        this.transaction = transaction;
        this.filter = filter;
    }

    public IndexMaintainerState(FDBRecordStore store,
                                Index index,
                                IndexMaintenanceFilter filter) {
        this(store, store.getRecordContext(), index, store.indexSubspace(index), store.ensureContextActive(), filter);
    }
}

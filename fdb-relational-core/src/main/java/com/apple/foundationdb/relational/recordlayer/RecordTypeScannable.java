/*
 * RecordTypeScannable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

public abstract class RecordTypeScannable<CursorT> implements DirectScannable {

    @SuppressWarnings("PMD.CloseResource") // cursor lifetime lasts into returned object's lifetime
    @Override
    public final ResumableIterator<Row> openScan(
            @Nullable Row keyPrefix,
            @Nonnull Options options) throws RelationalException {
        TupleRange range = TupleRange.allOf(TupleUtils.toFDBTuple(keyPrefix));
        BackingStore store = getSchema().loadStore();
        final RecordCursor<CursorT> cursor = openScan(store, range, options.getOption(Options.Name.CONTINUATION), options);
        return RecordLayerIterator.create(cursor, keyValueTransform());
    }

    /* ****************************************************************************************************************/
    /* abstract methods */

    protected abstract RecordCursor<CursorT> openScan(BackingStore store, TupleRange range,
                                                      @Nullable Continuation continuation, Options options) throws RelationalException;

    protected abstract RecordLayerSchema getSchema();

    protected abstract Function<CursorT, Row> keyValueTransform();

    protected abstract boolean supportsMessageParsing();

    protected abstract boolean hasConstantValueForPrimaryKey(Options options) throws RelationalException;
}

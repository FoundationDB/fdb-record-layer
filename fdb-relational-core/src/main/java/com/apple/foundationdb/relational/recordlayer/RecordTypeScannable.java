/*
 * RecordTypeScannable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class RecordTypeScannable<CursorT> implements DirectScannable {

    @Override
    public final ResumableIterator<Row> openScan(@Nonnull Transaction transaction,
                                                 @Nullable Row startKey,
                                                 @Nullable Row endKey,
                                                 @Nonnull Options options) throws RelationalException {
        TupleRange range;
        if (startKey == null) {
            if (endKey == null) {
                range = TupleRange.ALL; //this is almost certainly incorrect
            } else {
                range = TupleRange.between(null, TupleUtils.toFDBTuple(endKey));
            }
        } else if (endKey == null) {
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey), null);
        } else if (hasConstantValueForPrimaryKey() && startKey.equals(endKey)) {
            range = TupleRange.allOf(TupleUtils.toFDBTuple(startKey));
        } else {
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey), TupleUtils.toFDBTuple(endKey));
        }

        FDBRecordStore store = getSchema().loadStore();
        //TODO(bfines) get the type index for this
        ScanProperties props = QueryPropertiesUtils.getScanProperties(options);
        final RecordCursor<CursorT> cursor = openScan(store, range, options.getOption(Options.Name.CONTINUATION), props);
        return RecordLayerIterator.create(cursor, keyValueTransform());
    }

    /* ****************************************************************************************************************/
    /* abstract methods */

    protected abstract RecordCursor<CursorT> openScan(FDBRecordStore store, TupleRange range,
                                                      @Nullable Continuation continuation, ScanProperties props) throws RelationalException;

    protected abstract RecordLayerSchema getSchema();

    protected abstract Function<CursorT, Row> keyValueTransform();

    protected abstract boolean supportsMessageParsing();

    protected abstract boolean hasConstantValueForPrimaryKey() throws RelationalException;
}

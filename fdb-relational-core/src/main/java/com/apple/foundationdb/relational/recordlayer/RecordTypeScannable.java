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
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

public abstract class RecordTypeScannable<CURSOR_TYPE> implements Scannable {

    @Override
    public final Scanner<KeyValue> openScan(@Nonnull Transaction t,
                                            @Nullable NestableTuple startKey,
                                            @Nullable NestableTuple endKey,
                                            @Nonnull QueryProperties scanProperties) throws RelationalException {
        //TODO(bfines) this will need to be rewired to support continuations
        TupleRange range;
        if (startKey == null) {
            if (endKey == null) {
                range = TupleRange.ALL; //this is almost certainly incorrect
            } else {
                range = TupleRange.between(null, TupleUtils.toFDBTuple(endKey));
            }
        } else if (endKey == null) {
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey), null);
        } else {
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey), TupleUtils.toFDBTuple(endKey));
        }

        FDBRecordStore store = getSchema().loadStore();
        //TODO(bfines) get the type index for this
        ScanProperties props = QueryPropertiesUtils.getScanProperties(scanProperties);
        final RecordCursor<CURSOR_TYPE> cursor = openScan(store, range, props);
        return CursorScanner.create(cursor, keyValueTransform(), supportsMessageParsing());
    }

    /* ****************************************************************************************************************/
    /*abstract methods*/
    protected abstract RecordLayerSchema getSchema();

    protected abstract RecordCursor<CURSOR_TYPE> openScan(FDBRecordStore store, TupleRange range, ScanProperties props);

    protected abstract Function<CURSOR_TYPE, KeyValue> keyValueTransform();

    protected abstract boolean supportsMessageParsing();
}

/*
 * UnstoredRecord.java
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

import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An {@link FDBRecord} that doesn't actually link to any record store.
 * @param <M> record Message type
 *
 * For evaluation unit tests.
 */
public class UnstoredRecord<M extends Message> implements FDBRecord<M> {
    final M protoRecord;

    public UnstoredRecord(M protoRecord) {
        this.protoRecord = protoRecord;
    }

    @Nonnull
    @Override
    public Tuple getPrimaryKey() {
        throw new UnsupportedOperationException("this record does not have a primary key");
    }

    @Nonnull
    @Override
    public RecordType getRecordType() {
        throw new UnsupportedOperationException("this record does not have an associated record type");
    }

    @Nonnull
    @Override
    public M getRecord() {
        return protoRecord;
    }

    @Override
    public boolean hasVersion() {
        return false;
    }

    @Nullable
    @Override
    public FDBRecordVersion getVersion() {
        return null;
    }
}

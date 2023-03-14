/*
 * BackingStore.java
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;

import com.google.protobuf.Message;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

public interface BackingStore extends RecordMetaDataProvider {
    Optional<RelationalResultSet> executeQuery(EmbeddedRelationalConnection conn, String query, Options options) throws RelationalException;

    @Nullable
    Row get(Row key, Options options) throws RelationalException;

    @Nullable
    Row getFromIndex(Index index, Row key, Options options) throws RelationalException;

    boolean delete(Row key) throws RelationalException;

    void deleteRange(Map<String, Object> prefix, @Nullable String tableName) throws RelationalException;

    boolean insert(String tableName, Message message, boolean replaceOnDuplicate) throws RelationalException;

    RecordCursor<FDBStoredRecord<Message>> scanType(RecordType type, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException;

    RecordCursor<IndexEntry> scanIndex(Index index, TupleRange range, @Nullable Continuation continuation, Options options) throws RelationalException;

    default <T> T unwrap(Class<T> type) throws InternalErrorException {
        Class<? extends BackingStore> myClass = getClass();
        if (myClass.isAssignableFrom(type)) {
            return type.cast(this);
        } else {
            throw new InternalErrorException("Cannot unwrap instance of type <" + myClass.getCanonicalName() + "> as type <" + type.getCanonicalName() + ">");
        }
    }
}

/*
 * RecordSerializationValidationException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LoggableKeysAndValues;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exception class thrown by {@link RecordSerializer#validateSerialization(RecordMetaData, RecordType, Message, byte[], StoreTimer)}.
 * This indicates that there was a mismatch between the original record and its deserialized form. This indicates
 * that there is some kind of corruption during the serialization process, either because the serialized form is not
 * parseable or because the serialization process leads to fields being modified in unexpected ways.
 */
@API(API.Status.UNSTABLE)
@SuppressWarnings("serial")
public class RecordSerializationValidationException extends RecordCoreException {
    RecordSerializationValidationException(@Nonnull String message, @Nonnull RecordType recordType, @Nullable Tuple primaryKey, @Nullable Throwable cause) {
        super(message, cause);
        addLogInfo(LogMessageKeys.RECORD_TYPE, recordType);
        if (primaryKey != null) {
            addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
        }
        if (cause instanceof LoggableKeysAndValues<?>) {
            addLogInfo(((LoggableKeysAndValues<?>)cause).exportLogInfo());
        }
    }

    RecordSerializationValidationException(@Nonnull String message, @Nonnull RecordType recordType, @Nonnull Tuple primaryKey) {
        this(message, recordType, primaryKey, null);
    }
}

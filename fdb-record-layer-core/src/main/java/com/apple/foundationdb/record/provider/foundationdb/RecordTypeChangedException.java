/*
 * RecordTypeChangedException.java
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
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exception thrown when {@link FDBRecordStoreBase#updateRecord} is called, but the existing record has a different record type.
 * @see FDBRecordStoreBase.RecordExistenceCheck#ERROR_IF_RECORD_TYPE_CHANGED
 * @see FDBRecordStoreBase.RecordExistenceCheck#ERROR_IF_NOT_EXISTS_OR_RECORD_TYPE_CHANGED
 */
@SuppressWarnings("serial")
@API(API.Status.UNSTABLE)
public class RecordTypeChangedException extends RecordCoreException {
    public RecordTypeChangedException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }
}

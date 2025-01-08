/*
 * RecordStoreNoInfoAndNotEmptyException.java
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
import com.apple.foundationdb.record.RecordCoreStorageException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exception thrown when {@link FDBRecordStore.Builder#createOrOpen} is called, and the record store does
 * not have a store info header, but does have something else.
 * @see FDBRecordStoreBase.StoreExistenceCheck#ERROR_IF_NO_INFO_AND_NOT_EMPTY
 */
@SuppressWarnings("serial")
@API(API.Status.UNSTABLE)
public class RecordStoreNoInfoAndNotEmptyException extends RecordCoreStorageException {
    public RecordStoreNoInfoAndNotEmptyException(@Nonnull String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    public RecordStoreNoInfoAndNotEmptyException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }
}

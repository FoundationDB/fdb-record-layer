/*
 * UninitializedRecordStoreException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
 * An exception that can be thrown by {@link FDBRecordStore}s if they are used before they have been properly
 * initialized. In particular, this may be called if the user calls a method that requires information stored
 * within the record store's {@linkplain com.apple.foundationdb.record.RecordStoreState state} state (e.g., which
 * indexes have been built or information stored in the store's header), and the user has neither used a
 * variant of {@link FDBRecordStore.Builder#createOrOpen(FDBRecordStoreBase.StoreExistenceCheck)} to create the
 * store nor called {@link FDBRecordStore#checkVersion(FDBRecordStoreBase.UserVersionChecker, FDBRecordStoreBase.StoreExistenceCheck)}
 * manually. If a record store is accessed without calling one of those methods, then the record store can
 * end up in a corrupt state.
 *
 * <p>
 * If one encounters this error, it is generally a sign that they are doing something unsafe. To prevent
 * this error, the user may wish to audit the code for instances of calls to {@link FDBRecordStore.Builder#build()}
 * and {@link FDBRecordStore.Builder#uncheckedOpen()}, either of which may cause the user to make use of an
 * initialized record store.
 * </p>
 */
@API(API.Status.UNSTABLE)
public class UninitializedRecordStoreException extends RecordCoreException {
    private static final long serialVersionUID = 1L;

    UninitializedRecordStoreException(@Nonnull String msg, @Nullable Object... keyValues) {
        super(msg, keyValues);
    }
}

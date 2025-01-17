/*
 * UnsupportedRemoteFetchIndexException.java
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
 * Exception thrown when remote fetch is attempted on an index that does not support remote fetch.
 * This exception is to be thrown when an index maintainer that does not support remote fetch is asked to scan an index
 * using {@link IndexMaintainer#scanRemoteFetch}
 * <p>Note that this exception is caught and handled by {@link FDBRecordStore} and {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan}
 * and so should be thrown by any index maintainer (even outside this project) for proper handing of the error case.</p>
 */
@SuppressWarnings({"serial", "java:S110"})
@API(API.Status.EXPERIMENTAL)
public class UnsupportedRemoteFetchIndexException extends RecordCoreException {
    public UnsupportedRemoteFetchIndexException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }

    public UnsupportedRemoteFetchIndexException(Throwable cause) {
        super(cause);
    }

    public UnsupportedRemoteFetchIndexException(@Nonnull String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}

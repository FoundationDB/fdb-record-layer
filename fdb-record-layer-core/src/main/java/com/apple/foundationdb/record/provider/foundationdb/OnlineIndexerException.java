/*
 * MetaDataException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
 * Exception thrown when there is a problem in {@link com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer} during indexing.
 */
@SuppressWarnings("serial")
@API(API.Status.INTERNAL)
public class OnlineIndexerException extends RecordCoreException {
    public OnlineIndexerException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }
}

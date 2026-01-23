/*
 * DataNotAtLeafException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;

import javax.annotation.Nonnull;

/**
 * Exception to be thrown when attempting to save data at a path that is not a leaf.
 * This has, historically not been enforced, so there may be data already in this state.
 */
@API(API.Status.UNSTABLE)
public class DataNotAtLeafException extends RecordCoreException {
    private static final long serialVersionUID = 1L;

    public DataNotAtLeafException(@Nonnull final KeySpacePath path) {
        super("Data cannot be saved at non-leaf keyspace",
                LogMessageKeys.KEY_SPACE_PATH, path);
    }
}

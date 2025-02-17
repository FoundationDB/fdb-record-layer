/*
 * InternalErrorException.java
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

package com.apple.foundationdb.relational.api.exceptions;

import com.apple.foundationdb.annotation.API;

/**
 * Internal type of relational exception. Considered for removal to replace with using
 * {@link RelationalException} directly.
 */
@API(API.Status.EXPERIMENTAL)
public class InternalErrorException extends RelationalException {
    private static final long serialVersionUID = 1;

    public InternalErrorException(String message) {
        super(message, ErrorCode.INTERNAL_ERROR);
    }

    public InternalErrorException(String message, Throwable cause) {
        super(message, ErrorCode.INTERNAL_ERROR, cause);
    }
}

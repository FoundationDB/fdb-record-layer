/*
 * InvalidColumnReferenceException.java
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

@API(API.Status.EXPERIMENTAL)
public class InvalidColumnReferenceException extends RelationalException {
    public static final long serialVersionUID = 1L;
    private static final String MESSAGE_PREFIX_FOR_INVALID_POSITION_NUMBER = "Invalid column position number: ";

    public InvalidColumnReferenceException(String message) {
        super(message, ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    public static InvalidColumnReferenceException getExceptionForInvalidPositionNumber(int position) {
        return new InvalidColumnReferenceException(MESSAGE_PREFIX_FOR_INVALID_POSITION_NUMBER + position);
    }
}

/*
 * CloseException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.util;

import java.util.List;

/**
 * Exception thrown when the {@link CallbackUtils#invokeAll(List)} method catches an exception.
 * This exception will have the {@code cause} set to the first exception thrown during {@code invokeAll} and any further
 * exception thrown will be added as {@code Suppressed}.
 */
@SuppressWarnings("serial")
public class CallbackException extends RuntimeException {
    public CallbackException(final Throwable cause) {
        super(cause);
    }
}

/*
 * SynchronizedSessionLockedException.java
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

package com.apple.foundationdb.synchronizedsession;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.util.LoggableException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This exception means that the synchronized session is not valid anymore, probably because another synchronized session
 * on the same lock is running.
 * @see SynchronizedSession
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("serial")
public class SynchronizedSessionLockedException extends LoggableException {
    public SynchronizedSessionLockedException(@Nonnull String msg, @Nullable Object... keyValues) {
        super(msg, keyValues);
    }
}

/*
 * IsolationLevel.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

/**
 * The isolation level for reads from the database.
 *
 * @see ExecuteProperties#getIsolationLevel
 */
@API(API.Status.UNSTABLE)
public enum IsolationLevel {

    @API(API.Status.UNSTABLE)
    SNAPSHOT(true),

    @API(API.Status.UNSTABLE)
    SERIALIZABLE(false)
    ;

    private boolean isSnapshot;

    IsolationLevel(boolean isSnapshot) {
        this.isSnapshot = isSnapshot;
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }
}

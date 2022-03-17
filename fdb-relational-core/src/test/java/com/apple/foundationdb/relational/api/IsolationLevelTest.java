/*
 * IsolationLevelTest.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IsolationLevelTest {

    @Test
    void fromFdbLevel() throws RelationalException {
        assertThat(IsolationLevel.fromFdbLevel(com.apple.foundationdb.record.IsolationLevel.SERIALIZABLE)).isEqualTo(IsolationLevel.SERIALIZABLE);
        assertThat(IsolationLevel.fromFdbLevel(com.apple.foundationdb.record.IsolationLevel.SNAPSHOT)).isEqualTo(IsolationLevel.SNAPSHOT_ISOLATION);
    }

    @Test
    void valueOf() {
        assertThat(IsolationLevel.valueOf("READ_COMMITTED")).isEqualTo(IsolationLevel.READ_COMMITTED);
    }
}

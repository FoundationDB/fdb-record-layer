/*
 * RelationalTest.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RelationalTest {
    RelationalDriver driver = Mockito.mock(RelationalDriver.class);
    RelationalDriver driver2 = Mockito.mock(RelationalDriver.class);

    @Test
    void registerDriver() throws RelationalException {
        Relational.registerDriver(driver);
        RelationalAssertions.assertThrows(() -> Relational.registerDriver(driver2)).hasErrorCode(ErrorCode.PROTOCOL_VIOLATION);
        Relational.deregisterDriver(driver);
    }

    @Test
    void deregisterDriver() throws RelationalException {
        RelationalAssertions.assertThrows(() -> Relational.deregisterDriver(driver)).hasErrorCode(ErrorCode.PROTOCOL_VIOLATION);
        Relational.registerDriver(driver);
        Relational.deregisterDriver(driver);
        Relational.registerDriver(driver2);
        Relational.deregisterDriver(driver2);
    }
}

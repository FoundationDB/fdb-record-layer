/*
 * APIVersionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.test.ParameterizedTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class APIVersionTest {

    @SuppressWarnings("unused") // used as argument source for parameterized test
    static Stream<Arguments> isAtLeast() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(APIVersion.values()),
                Arrays.stream(APIVersion.values())
        );
    }

    @ParameterizedTest
    @MethodSource
    void isAtLeast(@Nonnull APIVersion version1, @Nonnull APIVersion version2) {
        assertEquals(version1.getVersionNumber() >= version2.getVersionNumber(), version1.isAtLeast(version2));
    }
}

/*
 * Copyright 2023 Christian Heina
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
 *
 * Modifications Copyright 2015-2025 Apple Inc. and the FoundationDB project authors.
 * This source file is part of the FoundationDB open source project
 */

package com.apple.foundationdb.half;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link HalfConstants}.
 *
 * @author Christian Heina (developer@christianheina.com)
 */
public class HalfConstantsTest {
    @Test
    public void constantsTest() {
        Assertions.assertEquals(HalfConstants.SIGNIFICAND_WIDTH, 11);
        Assertions.assertEquals(HalfConstants.MIN_SUB_EXPONENT, -24);
        Assertions.assertEquals(HalfConstants.EXP_BIAS, 15);

        Assertions.assertEquals(HalfConstants.SIGN_BIT_MASK, 0x8000);
        Assertions.assertEquals(HalfConstants.EXP_BIT_MASK, 0x7C00);
        Assertions.assertEquals(HalfConstants.SIGNIF_BIT_MASK, 0x03FF);

        // Check all bits filled and no overlap
        Assertions.assertEquals((HalfConstants.SIGN_BIT_MASK | HalfConstants.EXP_BIT_MASK | HalfConstants.SIGNIF_BIT_MASK), 65535);
        Assertions.assertEquals((HalfConstants.SIGN_BIT_MASK & HalfConstants.EXP_BIT_MASK), 0);
        Assertions.assertEquals((HalfConstants.SIGN_BIT_MASK & HalfConstants.SIGNIF_BIT_MASK), 0);
        Assertions.assertEquals((HalfConstants.EXP_BIT_MASK & HalfConstants.SIGNIF_BIT_MASK), 0);
    }
}

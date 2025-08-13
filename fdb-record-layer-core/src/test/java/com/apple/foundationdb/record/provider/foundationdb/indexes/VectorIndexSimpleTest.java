/*
 * MultidimensionalIndexTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for multidimensional type indexes.
 */
@Tag(Tags.RequiresFDB)
public class VectorIndexSimpleTest extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexSimpleTest.class);

    @Test
    void basicReadTest() throws Exception {
        super.basicReadTest(false);
    }

    @Test
    void basicConcurrentReadTest() throws Exception {
        super.basicConcurrentReadTest(false);
    }
}

/*
 * ValueHeightTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Random;

/**
 * This tests the calculation of {@link Value#height()}.
 */
public class ValueHeightTest {

    @Nonnull
    private static final Random random = new Random();

    @Nonnull
    private static Value valueOfDepth(int depth) {
        if (depth == 0) {
            return LiteralValue.ofScalar(random.nextInt(1000));
        }
        int childrenCount = random.nextInt(5) + 1;
        int branchingNodeIndex = random.nextInt(childrenCount);
        final ImmutableList.Builder<Value> childrenValuesBuilder = ImmutableList.builder();
        for (int i = 0; i < childrenCount; i++) {
            if (i == branchingNodeIndex) {
                childrenValuesBuilder.add(valueOfDepth(depth - 1));
            } else {
                childrenValuesBuilder.add(LiteralValue.ofScalar(random.nextInt(1000)));
            }
        }
        return RecordConstructorValue.ofUnnamed(childrenValuesBuilder.build());
    }

    @Test
    void valueHeightIsCalculatedCorrectly() {
        Assertions.assertEquals(0, valueOfDepth(0).height());
        for (int i = 0; i < 10000; i++) {
            final int depth = random.nextInt(100);
            Assertions.assertEquals(depth, valueOfDepth(depth).height());
        }
    }
}

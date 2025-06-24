/*
 * TreeDiameterTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Random;

public class TreeDiameterTest {

    @Nonnull
    private static final Random random = new Random();

    @Nonnull
    private static Value valueOfDepth(int depth) {
        if (depth == 0) {
            return LiteralValue.ofScalar(random.nextInt(1000));
        }
        int childrenCount = random.nextInt(5) + 1;
        int branchingNodeIndex = random.nextInt(childrenCount);
        final var childrenValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < childrenCount; i++) {
            if (i == branchingNodeIndex) {
                childrenValuesBuilder.add(valueOfDepth(depth - 1));
            } else {
                childrenValuesBuilder.add(LiteralValue.ofScalar(random.nextInt(1000)));
            }
        }
        return RecordConstructorValue.ofUnnamed(childrenValuesBuilder.build());
    }

    @Nonnull
    private static Value valueOfDiameter(int leftDepth, int rightDepth) {
        Verify.verify(leftDepth + rightDepth > 2);
        final var left = valueOfDepth(leftDepth);
        final var right = valueOfDepth(rightDepth);
        // add noise
        var randomChildrenCount = random.nextInt(5);
        final var childrenValuesBuilder = ImmutableList.<Value>builder();

        // add noise
        if (leftDepth > 1 && rightDepth > 1) {
            for (int i = 0; i < randomChildrenCount; i++) {
                childrenValuesBuilder.add(LiteralValue.ofScalar(random.nextInt(1000)));
            }
        }

        // add left child
        childrenValuesBuilder.add(left);

        // add noise
        if (leftDepth > 1 && rightDepth > 1) {
            for (int i = 0; i < randomChildrenCount; i++) {
                childrenValuesBuilder.add(LiteralValue.ofScalar(random.nextInt(1000)));
            }
        }

        // add right child
        childrenValuesBuilder.add(right);

        // add noise
        if (leftDepth > 1 && rightDepth > 1) {
            for (int i = 0; i < randomChildrenCount; i++) {
                childrenValuesBuilder.add(LiteralValue.ofScalar(random.nextInt(1000)));
            }
        }

        return RecordConstructorValue.ofUnnamed(childrenValuesBuilder.build());
    }

    @Test
    void testDiameterOfOrphanTree() {
        final var expectedDiameter = 0;
        final var actualValue = LiteralValue.ofScalar(42);
        Assertions.assertThat(actualValue.diameter()).isEqualTo(expectedDiameter);
    }

    @Test
    void testDiameterPassingThroughRoot() {
        final var expectedDiameter = 5 + 2;
        final var actualValue = valueOfDiameter(2, 3);
        Assertions.assertThat(actualValue.diameter()).isEqualTo(expectedDiameter);
    }

    @Test
    void testDiameterNotPassingThroughRoot() {
        final var expectedDiameter = 3 + 5 + 2;
        final var childTree = valueOfDiameter(3, 5);
        final var singleChildValue = RecordConstructorValue.ofUnnamed(ImmutableList.of(childTree));
        Assertions.assertThat(singleChildValue.diameter()).isEqualTo(expectedDiameter);
    }

    @Test
    void testLargestDiameterNotPassingThroughRoot() {
        final var smallChildTreeDiameter = 3 + 4 + 2;
        final var smallChildTree = valueOfDiameter(3, 4);
        final var largeChildTreeDiameter = 60 + 40 + 2;
        final var largeChildTree = valueOfDiameter(60, 40);
        final var singleChildValue = RecordConstructorValue.ofUnnamed(ImmutableList.of(smallChildTree, largeChildTree));
        Assertions.assertThat(singleChildValue.diameter()).isEqualTo(largeChildTreeDiameter);
    }

    @Test
    void testLargestDiameterPassingThroughRoot() {
        final var smallChildTree = valueOfDiameter(3, 30);
        final var largeChildTree = valueOfDiameter(6, 40);
        final var parent = RecordConstructorValue.ofUnnamed(ImmutableList.of(smallChildTree, largeChildTree));
        Assertions.assertThat(parent.diameter()).isEqualTo(30 + 1 + 40 + 1 + 2);
    }
}

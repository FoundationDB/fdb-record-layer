/*
 * LiteralsTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * Test suite for {@link Literals}. Currently, it only tests that no {@link OrderedLiteral}(s) are skipped when
 * building a {@link Literals} object.
 */
public class LiteralsTests {

    @Nonnull
    private final Random random = new Random(42L);

    @Test
    void nullLiteralsAreNotExcluded() {
        // TODO use RandomizedTestUtils.
        final var tokenIndices = generateUniqueIntegers(random, 0, 1000, ARGUMENTS_SIZE);
        final var nullLiteralsCount = 1 + random.nextInt(ARGUMENTS_SIZE - 1); // at least 1 null literal.
        final ArrayList<OrderedLiteral> orderedLiterals = new ArrayList<>(ARGUMENTS_SIZE);
        final var expectedNullOrderedLiterals = ImmutableList.<OrderedLiteral>builderWithExpectedSize(nullLiteralsCount);
        final var expectedNonNullOrderedLiterals = ImmutableList.<OrderedLiteral>builderWithExpectedSize(ARGUMENTS_SIZE - nullLiteralsCount);
        int nullLiteralsAdded = 0;
        for (final var tokenIndex : tokenIndices) {
            if (nullLiteralsAdded == nullLiteralsCount) {
                final var nonNullLiteral = nonNullLiteral(tokenIndex, random.nextInt() % 10000);
                orderedLiterals.add(nonNullLiteral);
                expectedNonNullOrderedLiterals.add(nonNullLiteral);
            } else {
                final var nullLiteral = nullLiteral(tokenIndex);
                orderedLiterals.add(nullLiteral(tokenIndex));
                expectedNullOrderedLiterals.add(nullLiteral);
                nullLiteralsAdded++;
            }
        }
        Collections.shuffle(orderedLiterals);

        final var literalsBuilder = Literals.newBuilder();
        orderedLiterals.forEach(literalsBuilder::addLiteral);
        final var actualLiterals = literalsBuilder.build();
        final var actualAsMap = actualLiterals.asMap();

        final HashMap<String, Object> expectedMap = new HashMap<>();
        expectedNullOrderedLiterals.build().forEach(l -> expectedMap.put(l.getConstantId(), l.getLiteralObject()));
        expectedNonNullOrderedLiterals.build().forEach(l -> expectedMap.put(l.getConstantId(), l.getLiteralObject()));

        Assertions.assertEquals(expectedMap, actualAsMap);
    }

    @Nonnull
    private static OrderedLiteral nonNullLiteral(int tokenIndex, int value) {
        return new OrderedLiteral(Type.primitiveType(Type.TypeCode.LONG), value, null, null, tokenIndex, Optional.empty());
    }

    @Nonnull
    private static OrderedLiteral nullLiteral(int tokenIndex) {
        return new OrderedLiteral(Type.primitiveType(Type.TypeCode.NULL), null /*literal value*/, null, null, tokenIndex, Optional.empty());
    }

    private static final int ARGUMENTS_SIZE = 100;

    @Nonnull
    public static Set<Integer> generateUniqueIntegers(@Nonnull final Random random, int min, int max, int count) {
        Assert.thatUnchecked(0 <= min && min < max, "invalid range boundaries");
        Assert.thatUnchecked(count > 0 && count <= (max - min), "pick values' range is smaller than the desired number of unique values");
        return random.ints(min, max + 1)
                .distinct()
                .limit(count)
                .boxed()
                .collect(ImmutableSet.toImmutableSet());
    }
}

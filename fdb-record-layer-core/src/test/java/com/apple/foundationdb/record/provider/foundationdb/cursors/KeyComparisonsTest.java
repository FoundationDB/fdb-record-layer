/*
 * KeyComparisonsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link KeyComparisons}.
 */
public class KeyComparisonsTest {
    private static int normalizeComparison(int comparison) {
        return (comparison > 0) ? 1 : ((comparison < 0) ? -1 : 0);
    }

    private static String randomString(@Nonnull Random r) {
        int length = (int)(Math.abs(r.nextGaussian() * 10));
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < length; j++) {
            if (r.nextBoolean()) {
                sb.append((char)('a' + r.nextInt(26)));
            } else {
                sb.append((char)('A' + r.nextInt(26)));
            }
        }
        return sb.toString();
    }

    private static <E> void testFieldComparator(@Nonnull List<E> list) {
        for (E elem1 : list) {
            for (E elem2 : list) {
                int actualComparison = normalizeComparison(KeyComparisons.FIELD_COMPARATOR.compare(elem1, elem2));
                int expectedComparison = normalizeComparison(ByteArrayUtil.compareUnsigned(
                        Tuple.from(elem1).pack(), Tuple.from(elem2).pack()
                ));
                assertEquals(expectedComparison, actualComparison, "Comparison does not match for elem1 and elem2");
            }
        }
    }

    @Test
    @Tag(Tags.Slow)
    public void fieldComparator() {
        Random r = new Random(0xba5eba11);

        // Type 1: Integers
        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            integers.add(r.nextInt());
        }
        integers.add(null);
        testFieldComparator(integers);

        // Type 2: Strings
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            strings.add(randomString(r));
        }
        strings.add(null);
        testFieldComparator(strings);

        // Type 3: Booleans
        testFieldComparator(Arrays.asList(null, Boolean.TRUE, Boolean.FALSE));

        // Type 4: Doubles
        List<Double> doubles = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            doubles.add(r.nextGaussian());
        }
        doubles.add(null);
        testFieldComparator(doubles);

        // Type 5: Byte Arrays
        List<byte[]> byteArrays = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int length = (int)(Math.abs(r.nextGaussian() + 0.2) * 10);
            byte[] arr = new byte[length];
            r.nextBytes(arr);
            byteArrays.add(arr);
        }
        byteArrays.add(null);
        testFieldComparator(byteArrays);

        // Type 6: Lists of integers.
        List<List<Integer>> intLists = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int length = (int)(Math.abs(r.nextGaussian() + 0.2) * 10);
            List<Integer> list = new ArrayList<>();
            for (int j = 0; j < length; j++) {
                list.add(r.nextInt());
                if (r.nextDouble() < 0.3) {
                    intLists.add(new ArrayList<>(list));
                }
            }
            intLists.add(list);
        }
        intLists.add(null);
        testFieldComparator(intLists);

        // Type 7: Lists of strings.
        List<List<String>> strLists = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int length = (int)(Math.abs(r.nextGaussian() + 0.2) * 10);
            List<String> list = new ArrayList<>();
            for (int j = 0; j < length; j++) {
                list.add(randomString(r));
                if (r.nextDouble() < 0.3) {
                    strLists.add(new ArrayList<>(list));
                }
            }
            strLists.add(list);
        }
        strLists.add(null);
        testFieldComparator(strLists);
    }

    private static void testKeyComparator(@Nonnull List<Key.Evaluated> keys) {
        for (Key.Evaluated key1 : keys) {
            for (Key.Evaluated key2 : keys) {
                int actualComparison = normalizeComparison(KeyComparisons.KEY_COMPARATOR.compare(key1.toList(), key2.toList()));
                int expectedComparison = normalizeComparison(ByteArrayUtil.compareUnsigned(
                        key1.toTuple().pack(),
                        key2.toTuple().pack()
                ));
                assertEquals(expectedComparison, actualComparison, "Comparison did not match for " + key1 + " and " + key2);
            }
        }
    }

    @Test
    @Tag(Tags.Slow)
    public void keyComparator() {
        Random r = new Random(0x5ca1ab1e);

        // Type 1: Integers.
        List<Key.Evaluated> intKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            intKeys.add(Key.Evaluated.scalar(r.nextInt()));
        }
        intKeys.add(Key.Evaluated.NULL);
        testKeyComparator(intKeys);

        // Type 2: Int-Strings
        List<Key.Evaluated> intStringKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            intStringKeys.add(Key.Evaluated.concatenate(r.nextInt(5), randomString(r)));
        }
        intStringKeys.add(Key.Evaluated.NULL);
        testKeyComparator(intStringKeys);

        // Type 3: Int-String lists
        List<Key.Evaluated> intStringListKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int length = (int)(Math.abs(r.nextGaussian() + 0.2) * 10);
            int first = r.nextInt(5);
            List<String> list = new ArrayList<>();
            for (int j = 0; j < length; j++) {
                list.add(randomString(r));

                if (r.nextDouble() < 0.4) {
                    intStringListKeys.add(Key.Evaluated.concatenate(first, new ArrayList<>(list)));
                }
            }
            intStringListKeys.add(Key.Evaluated.concatenate(first, list));
        }
        intStringListKeys.add(Key.Evaluated.NULL);
        testKeyComparator(intStringListKeys);

        // Type 4: Int-Byte arrays
        List<Key.Evaluated> intByteArraysKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int length = (int)(Math.abs(r.nextGaussian() + 0.2) * 10);
            byte[] arr = new byte[length];
            r.nextBytes(arr);
            intByteArraysKeys.add(Key.Evaluated.concatenate(r.nextInt(5), arr));
        }
        intByteArraysKeys.add(Key.Evaluated.NULL);
        testKeyComparator(intByteArraysKeys);

        // Type 5: Mix of ints and int byte arrays.
        List<Key.Evaluated> mixedKeys = new ArrayList<>();
        mixedKeys.addAll(intKeys);
        mixedKeys.addAll(intByteArraysKeys);
        for (Key.Evaluated key : intByteArraysKeys) {
            List<Object> list = key.toList();
            if (list.size() > 0) {
                mixedKeys.add(Key.Evaluated.scalar(list.get(0)));
            }
        }
        testKeyComparator(mixedKeys);
    }
}

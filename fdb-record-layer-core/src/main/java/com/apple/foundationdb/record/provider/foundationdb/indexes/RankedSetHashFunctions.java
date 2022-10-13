/*
 * RankedSetHashFunctions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.Nonnull;

/**
 * Known hash functions available as index options.
 */
@API(API.Status.EXPERIMENTAL)
public class RankedSetHashFunctions {
    public static final String JDK = "JDK";
    public static final String CRC = "CRC";
    public static final String RANDOM = "RANDOM";
    public static final String MURMUR3 = "MURMUR3";

    @SuppressWarnings("UnstableApiUsage")
    private static final RankedSet.HashFunction MURMUR3_HASH_FUNCTION = new GuavaHashFunction(Hashing.murmur3_32_fixed());

    private static final BiMap<String, RankedSet.HashFunction> extent = ImmutableBiMap.<String, RankedSet.HashFunction>builder()
            .put(JDK, RankedSet.JDK_ARRAY_HASH)
            .put(CRC, RankedSet.CRC_HASH)
            .put(RANDOM, RankedSet.RANDOM_HASH)
            .put(MURMUR3, MURMUR3_HASH_FUNCTION)
            .build();

    public static RankedSet.HashFunction getHashFunction(@Nonnull String name) {
        RankedSet.HashFunction result = extent.get(name);
        if (result != null) {
            return result;
        }
        throw new RecordCoreArgumentException("hash function not found: " + name);
    }

    public static String getHashFunctionName(@Nonnull RankedSet.HashFunction hashFunction) {
        return extent.inverse().get(hashFunction);
    }

    private RankedSetHashFunctions() {
    }

    /**
     * Use {@code com.google.common.hash.HashFunction}s as {@code RankedSet.HashFunction}s.
     */
    private static class GuavaHashFunction implements RankedSet.HashFunction {
        @Nonnull
        private final HashFunction guava;

        public GuavaHashFunction(HashFunction guava) {
            this.guava = guava;
        }

        @Override
        public int hash(byte[] key) {
            return guava.hashBytes(key).asInt();
        }
    }
}

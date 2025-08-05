/*
 * UniformDataSource.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest.datagen;

import java.util.Random;

/**
 * A uniformly-distributed random data source based on java.util.Random.
 */
public class UniformDataSource implements RandomDataSource {
    private final Random random;
    private final int maxStringLength;
    private final int maxBytesLength;

    public UniformDataSource(Random random, int maxStringLength, int maxBytesLength) {
        this.random = random;
        this.maxStringLength = maxStringLength;
        this.maxBytesLength = maxBytesLength;
    }

    public UniformDataSource(long seed, int maxStringLength, int maxBytesLength) {
        this.random = new Random(seed);
        this.maxStringLength = maxStringLength;
        this.maxBytesLength = maxBytesLength;
    }

    public UniformDataSource(int maxStringLength, int maxBytesLength) {
        this.random = new Random();
        this.maxStringLength = maxStringLength;
        this.maxBytesLength = maxBytesLength;
    }

    @Override
    public int nextInt() {
        return random.nextInt();
    }

    @Override
    public int nextInt(int min, int max) {
        if (max == min) {
            return min;
        }
        return random.nextInt(max - min) + min;
    }

    @Override
    public long nextLong() {
        return random.nextLong();
    }

    @Override
    public float nextFloat() {
        return random.nextFloat();
    }

    @Override
    public double nextDouble() {
        return random.nextDouble();
    }

    @Override
    public String nextUtf8() {
        int stringLength = random.nextInt(maxStringLength);
        return nextUtf8(stringLength);
    }

    @Override
    public byte[] nextBytes() {
        int length = random.nextInt(maxBytesLength);
        return nextBytes(length);
    }

    @Override
    public byte[] nextBytes(int byteLength) {
        byte[] bytes = new byte[byteLength];
        random.nextBytes(bytes);
        return bytes;
    }

    @Override
    public boolean nextBoolean() {
        return random.nextBoolean();
    }
}

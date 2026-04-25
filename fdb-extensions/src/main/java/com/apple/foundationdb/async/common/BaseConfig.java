/*
 * BaseConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.linear.Metric;

import javax.annotation.Nonnull;

/**
 * TODO.
 */
@SuppressWarnings("checkstyle:MemberName")
public interface BaseConfig {
    /**
     * The metric that is used to determine distances between vectors.
     */
    @Nonnull
    Metric getMetric();

    /**
     * The number of dimensions used. All vectors must have exactly this number of dimensions.
     */
    int getNumDimensions();

    boolean isUseRaBitQ();

    /**
     * Number of bits per dimensions iff {@link #isUseRaBitQ()} is set to {@code true}, ignored otherwise. If RaBitQ
     * encoding is used, a vector is stored using roughly {@code 25 + numDimensions * (numExBits + 1) / 8} bytes.
     */
    int getRaBitQNumExBits();
}

/*
 * Quantizer.java
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

package com.apple.foundationdb.async.hnsw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public interface Quantizer {
    Logger logger = LoggerFactory.getLogger(Quantizer.class);

    @Nonnull
    Estimator estimator();

    @Nonnull
    Vector encode(@Nonnull final Vector data);

    @Nonnull
    static Quantizer noOpQuantizer(@Nonnull final Metrics metric) {
        return new Quantizer() {
            @Nonnull
            @Override
            public Estimator estimator() {
                return (vector1, vector2) -> {
                    final double d = metric.comparativeDistance(vector1, vector2);
                    //logger.info("estimator distance = {}", d);
                    return d;
                };
            }

            @Nonnull
            @Override
            public Vector encode(@Nonnull final Vector data) {
                return data;
            }
        };
    }
}

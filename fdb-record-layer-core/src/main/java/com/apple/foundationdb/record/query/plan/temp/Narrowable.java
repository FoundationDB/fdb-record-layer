/*
 * Narrowable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Narrow-able trait that can safely downcast to a particular implementation. Should be mixed in if supported.
 * @param <T> type parameter
 */
public interface Narrowable<T> {

    /**
     * Safe-casts the {@link Narrowable} instance to another type.
     *
     * @param clazz marker object.
     * @return if cast is successful, an {@link Optional} containing the instance cast to {@link T}, otherwise an
     * empty {@link Optional}.
     */
    default <T1 extends T> Optional<T1> narrowMaybe(@Nonnull final Class<T1> clazz) {
        if (clazz.isInstance(this)) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }
}

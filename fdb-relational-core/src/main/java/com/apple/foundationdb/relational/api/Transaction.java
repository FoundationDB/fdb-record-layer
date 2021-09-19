/*
 * Transaction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;

public interface Transaction extends AutoCloseable {

    void commit();

    void abort();

    @Override
    void close() throws RelationalException;

    /**
     * Unwraps this instance as type T, if such a cast is possible. This provides a convenient API
     * for unwrapping implementation calls from the interface (to avoid lots of instanceof checks everywhere).
     *
     * @param type the type to unwrap it as.
     * @param <T>  the generic type
     * @return this instance, as an instanceof Type T
     */
    @Nonnull
    default <T> T unwrap(@Nonnull Class<? extends T> type) throws InternalErrorException {
        Class<? extends Transaction> myClass = this.getClass();
        if (myClass.isAssignableFrom(type)) {
            return type.cast(this);
        } else {
            throw new InternalErrorException("Cannot unwrap instance of type <" + myClass.getCanonicalName() + "> as type <" + type.getCanonicalName() + ">");
        }
    }
}

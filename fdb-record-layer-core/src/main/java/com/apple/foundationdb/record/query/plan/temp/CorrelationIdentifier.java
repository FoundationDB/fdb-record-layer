/*
 * CorrelationIdentifier.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.UUID;

/**
 * A correlation identifier is an immutable object that is created with a string uniquely identifying it.
 */
@API(API.Status.EXPERIMENTAL)
public class CorrelationIdentifier {
    @Nonnull private final String id;

    /**
     * Create a new correlation identifier using the given string. It is the callers responsibility to only use
     * unique string values in order to avoid clashes.
     * @param id the identifier string
     * @return a new {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier of(@Nonnull final String id) {
        return new CorrelationIdentifier(id);
    }

    /**
     * Create a new correlation identifier using a random string. The returned correlation identifier can be assumed
     * to be unique.
     * @return a new {@link CorrelationIdentifier}
     */
    @Nonnull
    public static CorrelationIdentifier randomID() {
        return new CorrelationIdentifier(UUID.randomUUID().toString());
    }

    private CorrelationIdentifier(@Nonnull final String id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CorrelationIdentifier that = (CorrelationIdentifier)o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Returns the backing string id.
     * @return the string backing this {@link CorrelationIdentifier}
     */
    @Override
    public String toString() {
        return id;
    }
}

/*
 * ValueRange.java
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

package com.apple.foundationdb.record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A range defines the boundaries around a contiguous span of values of some type.
 * @param <T> the type of values defined by this range.
 */
public class ValueRange<T> {
    public static final ValueRange<?> ALL = new ValueRange<>(null, null, EndpointType.TREE_START, EndpointType.TREE_END);
    @Nullable
    private final T low;
    @Nullable
    private final T high;
    @Nonnull
    private final EndpointType lowEndpoint;
    @Nonnull
    private final EndpointType highEndpoint;

    public ValueRange(@Nullable T low, @Nullable T high,
                      @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        this.low = low;
        this.high = high;
        this.lowEndpoint = lowEndpoint;
        this.highEndpoint = highEndpoint;
    }

    @Nullable
    public T getLow() {
        return low;
    }

    @Nullable
    public T getHigh() {
        return high;
    }

    @Nonnull
    public EndpointType getLowEndpoint() {
        return lowEndpoint;
    }

    @Nonnull
    public EndpointType getHighEndpoint() {
        return highEndpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValueRange<?> that = (ValueRange<?>)o;
        return Objects.equals(low, that.low) &&
               Objects.equals(high, that.high) &&
               lowEndpoint == that.lowEndpoint &&
               highEndpoint == that.highEndpoint;
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, high, lowEndpoint, highEndpoint);
    }

    @Override
    public String toString() {
        return lowEndpoint.toString(false) + low +
               "," + high + highEndpoint.toString(true);
    }
}

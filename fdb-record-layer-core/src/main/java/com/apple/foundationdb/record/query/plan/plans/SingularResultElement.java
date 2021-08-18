/*
 * LiteralResultElement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A representation of a {@link QueryResultElement} that contains a single value: Integer, Long etc.
 *
 * @param <T> teh type of literal
 */
public class SingularResultElement<T> implements QueryResultElement {
    @Nonnull
    private final T value;

    private SingularResultElement(final @Nonnull T value) {
        this.value = value;
    }

    public static SingularResultElement<Integer> of(Integer value) {
        return new SingularResultElement<>(value);
    }

    public static SingularResultElement<Long> of(Long value) {
        return new SingularResultElement<>(value);
    }

    public static SingularResultElement<Float> of(Float value) {
        return new SingularResultElement<>(value);
    }

    public static SingularResultElement<Double> of(Double value) {
        return new SingularResultElement<>(value);
    }

    public static SingularResultElement<String> of(String value) {
        return new SingularResultElement<>(value);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Class<T> getResultElementType() {
        return (Class<T>)value.getClass();
    }

    @Nonnull
    @Override
    public T getResultElement() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SingularResultElement<?> that = (SingularResultElement<?>)o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "LiteralResultElement: value=" + value;
    }
}

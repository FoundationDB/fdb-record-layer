/*
 * RecordLayerPropertyKey.java
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

package com.apple.foundationdb.record.provider.foundationdb.properties;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A key for a property that adopter configures for Record Layer.
 * This is not supposed to be defined by adopter outside Record Layer.
 * The name, value type, and its default value are defined in this key, which are immutable.
 * Call {@link #buildValue(Supplier)} with a {@link Supplier} of the value to build a {@link RecordLayerPropertyValue},
 * to override the value for the property, and then build a {@link RecordLayerPropertyStorage} with the new property value.
 *
 * @param <T> the type of the property's value
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerPropertyKey<T> {
    @Nonnull
    private final String name;
    @Nullable
    private final T defaultValue;
    @Nonnull
    private final Class<T> type;

    public RecordLayerPropertyKey(@Nonnull final String name, @Nullable T defaultValue, @Nonnull Class<T> type) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.type = type;
    }

    /**
     * The name for each property needs to be unique and in a reverse FQDN format according to the package it is located.
     *
     * @return the name of this property
     */
    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public Class<T> getType() {
        return type;
    }

    @Nullable
    public T getDefaultValue() {
        return defaultValue;
    }

    @Nonnull
    public RecordLayerPropertyValue<T> buildValue(@Nonnull Supplier<T> valueSupplier) {
        return new RecordLayerPropertyValue<>(this, valueSupplier);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }
        RecordLayerPropertyKey<?> otherKey = (RecordLayerPropertyKey<?>) other;
        return this.name.equals(otherKey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    public static RecordLayerPropertyKey<Boolean> booleanPropertyKey(@Nonnull final String name, final boolean defaultValue) {
        return new RecordLayerPropertyKey<>(name, defaultValue, Boolean.class);
    }

    public static RecordLayerPropertyKey<String> stringPropertyKey(@Nonnull final String name, @Nonnull final String defaultValue) {
        return new RecordLayerPropertyKey<>(name, defaultValue, String.class);
    }

    public static RecordLayerPropertyKey<Integer> integerPropertyKey(@Nonnull final String name, final int defaultValue) {
        return new RecordLayerPropertyKey<>(name, defaultValue, Integer.class);
    }

    public static RecordLayerPropertyKey<Long> longPropertyKey(@Nonnull final String name, final long defaultValue) {
        return new RecordLayerPropertyKey<>(name, defaultValue, Long.class);
    }

    public static RecordLayerPropertyKey<Double> doublePropertyKey(@Nonnull final String name, final double defaultValue) {
        return new RecordLayerPropertyKey<>(name, defaultValue, Double.class);
    }
}

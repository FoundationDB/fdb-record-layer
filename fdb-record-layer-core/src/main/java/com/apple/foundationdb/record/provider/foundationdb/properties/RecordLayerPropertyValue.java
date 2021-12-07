/*
 * RecordLayerPropertyValue.java
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
import java.util.function.Supplier;

/**
 * A value and its associated {@link RecordLayerPropertyKey} for a property that adopter configures for Record Layer.
 * Adopters can populate this value with providing a {@link #valueSupplier}.
 * The {@link #key} and its name, type and default value are supposed to completely defined by Record Layer.
 * The value is lazily loaded from the supplier when the property is read.
 * If no supplier is provided, a default value is used.
 * If the default value defined by Record Layer is supposed to be used, no need to put it into {@link RecordLayerPropertyStorage},
 * Record Layer can automatically read its default one.
 *
 * @param <T> the type of the property's value
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerPropertyValue<T> {
    @Nonnull
    private final RecordLayerPropertyKey<T> key;
    @Nullable
    private final Supplier<T> valueSupplier;

    RecordLayerPropertyValue(@Nonnull RecordLayerPropertyKey<T> key, @Nullable Supplier<T> valueSupplier) {
        this.key = key;
        this.valueSupplier = valueSupplier;
    }

    @Nonnull
    public RecordLayerPropertyKey<T> getKey() {
        return key;
    }

    @Nullable
    public T getValue() {
        final T defaultValue = key.getDefaultValue();
        if (valueSupplier == null) {
            return defaultValue;
        }
        return valueSupplier.get();
    }
}

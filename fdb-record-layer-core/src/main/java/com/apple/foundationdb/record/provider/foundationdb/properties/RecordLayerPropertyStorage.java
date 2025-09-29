/*
 * RecordLayerPropertyStorage.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Adopter can use this mapping of {@link RecordLayerPropertyKey}s to {@link RecordLayerPropertyValue}s to configure Record Layer parameters.
 * The instances of {@link RecordLayerPropertyKey} and how they are used are defined in Record Layer.
 * Adopters can populate {@link RecordLayerPropertyValue} and put them into this storage.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordLayerPropertyStorage {
    private static final RecordLayerPropertyStorage EMPTY_PROPERTY_STORAGE = new RecordLayerPropertyStorage(ImmutableMap.of());

    @Nonnull
    private final ImmutableMap<RecordLayerPropertyKey<?>, RecordLayerPropertyValue<?>> propertyMap;

    private RecordLayerPropertyStorage(@Nonnull ImmutableMap<RecordLayerPropertyKey<?>, RecordLayerPropertyValue<?>> propertyMap) {
        this.propertyMap = propertyMap;
    }

    @Nonnull
    public ImmutableMap<RecordLayerPropertyKey<?>, RecordLayerPropertyValue<?>> getPropertyMap() {
        return propertyMap;
    }

    @Nullable
    public <T> T getPropertyValue(@Nonnull RecordLayerPropertyKey<T> propertyKey) {
        if (propertyMap.containsKey(propertyKey)) {
            Object value = Preconditions.checkNotNull(propertyMap.get(propertyKey)).getValue();
            try {
                return propertyKey.getType().cast(value);
            } catch (ClassCastException ex) {
                throw new RecordCoreException("Invalid type for record context property", ex)
                        .addLogInfo(LogMessageKeys.PROPERTY_NAME, propertyKey.getName(),
                                LogMessageKeys.PROPERTY_TYPE, value.getClass().getName());
            }
        } else {
            return propertyKey.getDefaultValue();
        }
    }

    @Nonnull
    public Builder toBuilder() {
        return new Builder(propertyMap);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static RecordLayerPropertyStorage getEmptyInstance() {
        return EMPTY_PROPERTY_STORAGE;
    }

    /**
     * Builder for {@code RecordLayerPropertyStorage}.
     */
    public static class Builder {
        private final Map<RecordLayerPropertyKey<?>, RecordLayerPropertyValue<?>> propertyMap;

        private Builder() {
            this.propertyMap = new HashMap<>();
        }

        private Builder(ImmutableMap<RecordLayerPropertyKey<?>, RecordLayerPropertyValue<?>> properties) {
            this.propertyMap = new HashMap<>(properties);
        }

        public <T> boolean hasProp(@Nonnull RecordLayerPropertyKey<T> propKey) {
            return propertyMap.containsKey(propKey);
        }

        public <T> void removeProp(@Nonnull RecordLayerPropertyKey<T> propKey) {
            propertyMap.remove(propKey);
        }

        public <T> Builder addProp(@Nonnull RecordLayerPropertyValue<T> propValue) {
            if (this.propertyMap.putIfAbsent(propValue.getKey(), propValue) != null) {
                throw new RecordCoreException("Duplicate property name is added")
                        .addLogInfo(LogMessageKeys.PROPERTY_NAME, propValue.getKey().getName());
            }
            return this;
        }

        public <T> Builder addProp(@Nonnull RecordLayerPropertyKey<T> propKey, @Nonnull Supplier<T> valueSupplier) {
            final RecordLayerPropertyValue<T> propValue = propKey.buildValue(valueSupplier);
            return this.addProp(propValue);
        }

        public <T> Builder addProp(@Nonnull RecordLayerPropertyKey<T> propKey, @Nonnull T value) {
            final RecordLayerPropertyValue<T> propValue = propKey.buildValue(() -> value);
            return this.addProp(propValue);
        }

        public RecordLayerPropertyStorage build() {
            return new RecordLayerPropertyStorage(ImmutableMap.copyOf(propertyMap));
        }
    }
}

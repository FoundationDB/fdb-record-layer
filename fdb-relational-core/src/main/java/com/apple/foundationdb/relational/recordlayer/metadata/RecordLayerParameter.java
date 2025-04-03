/*
 * RecordLayerParameter.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Parameter;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class RecordLayerParameter implements Parameter {

    @Nonnull
    private final Mode mode;

    @Nonnull
    private final Optional<String> name;

    @Nonnull
    private final DataType dataType;

    @Nonnull
    private final Optional<Value> defaultValue;

    public RecordLayerParameter(@Nonnull final Mode mode,
                                @Nonnull final Optional<String> name,
                                @Nonnull final DataType dataType,
                                @Nonnull final Optional<Value> defaultValue) {
        this.mode = mode;
        this.name = name;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    @Nonnull
    @Override
    public Mode getMode() {
        return mode;
    }

    @Nonnull
    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean hasDefaultValue() {
        return defaultValue.isPresent();
    }

    @Nonnull
    @Override
    public Value getDefaultValue() {
        return Assert.optionalUnchecked(defaultValue);
    }

    @Override
    public boolean isNamed() {
        return name.isPresent();
    }

    @Nonnull
    @Override
    public String getName() {
        return Assert.optionalUnchecked(name);
    }

    @Nonnull
    public Optional<String> getNameMaybe() {
        return name;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RecordLayerParameter that = (RecordLayerParameter)o;
        return Objects.equals(getNameMaybe(), that.getNameMaybe())
                && mode == that.mode
                && Objects.equals(getDataType(), that.getDataType())
                && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNameMaybe(), mode, getDataType(), defaultValue);
    }

    public static final class Builder {
        private String name;
        private Parameter.Mode mode;
        private DataType dataType;
        private Optional<Value> defaultValue;

        private Builder() {
            name = null;
            mode = Mode.IN;
            defaultValue = Optional.empty();
        }

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setMode(@Nonnull final Parameter.Mode mode) {
            this.mode = mode;
            return this;
        }

        @Nonnull
        public Builder setDataType(@Nonnull final DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        @Nonnull
        public Builder setDefaultValue(@Nonnull final Value defaultValue) {
            this.defaultValue = Optional.of(defaultValue);
            return this;
        }

        @Nonnull
        public RecordLayerParameter build() {
            Assert.notNullUnchecked(dataType);
            Assert.notNullUnchecked(mode);
            return new RecordLayerParameter(mode, Optional.ofNullable(name), dataType, defaultValue);
        }
    }
}

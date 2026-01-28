/*
 * RecordLayerInvokedRoutine.java
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

import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.util.MemoizedFunction;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Function;

public class RecordLayerInvokedRoutine implements InvokedRoutine {

    @Nonnull
    private final String description;

    @Nonnull
    private final String normalizedDescription;

    @Nonnull
    private final PreparedParams preparedParams;

    @Nonnull
    private final String name;

    private final boolean isTemporary;

    @Nonnull
    private final Function<Boolean, UserDefinedFunction> userDefinedFunctionProvider;

    @Nonnull
    private final UserDefinedFunction serializableFunction;

    public RecordLayerInvokedRoutine(@Nonnull final String description,
                                     @Nonnull final String normalizedDescription,
                                     @Nonnull final String name,
                                     @Nonnull final PreparedParams preparedParams,
                                     boolean isTemporary,
                                     @Nonnull final Function<Boolean, UserDefinedFunction> userDefinedFunctionProvider,
                                     @Nonnull final UserDefinedFunction serializableFunction) {
        this.description = description;
        this.normalizedDescription = normalizedDescription;
        this.name = name;
        this.preparedParams = preparedParams;
        this.isTemporary = isTemporary;
        this.userDefinedFunctionProvider = MemoizedFunction.memoize(userDefinedFunctionProvider::apply);
        this.serializableFunction = serializableFunction;
    }

    @Nonnull
    @Override
    public String getDescription() {
        return description;
    }

    @Nonnull
    @Override
    public String getNormalizedDescription() {
        return normalizedDescription;
    }

    @Nonnull
    public PreparedParams getPreparedParams() {
        return preparedParams;
    }

    @Nonnull
    public Function<Boolean, UserDefinedFunction> getUserDefinedFunctionProvider() {
        return userDefinedFunctionProvider;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public UserDefinedFunction asSerializableFunction() {
        return serializableFunction;
    }

    @Override
    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RecordLayerInvokedRoutine that = (RecordLayerInvokedRoutine)o;
        return Objects.equals(description, that.description) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, name);
    }

    @Override
    public String toString() {
        return "invoked routine (name '" + name + "', description '" + description + "')";
    }

    @Nonnull
    public Builder toBuilder() {
        return newBuilder()
                .setName(getName())
                .setDescription(getDescription())
                .setNormalizedDescription(getNormalizedDescription())
                .setTemporary(isTemporary())
                .withUserDefinedFunctionProvider(getUserDefinedFunctionProvider())
                .withSerializableFunction(asSerializableFunction());
    }

    public static final class Builder {
        private String description;
        private String normalizedDescription;
        private PreparedParams preparedParams;
        private String name;
        private Function<Boolean, UserDefinedFunction> userDefinedFunctionProvider;
        private UserDefinedFunction serializableFunction;
        private boolean isTemporary;

        private Builder() {
        }

        @Nonnull
        public Builder setDescription(@Nonnull final String description) {
            this.description = description;
            return this;
        }

        @Nonnull
        public Builder setNormalizedDescription(@Nonnull final String normalizedDescription) {
            this.normalizedDescription = normalizedDescription;
            return this;
        }

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder withUserDefinedFunctionProvider(@Nonnull final Function<Boolean, UserDefinedFunction> userDefinedFunctionProvider) {
            this.userDefinedFunctionProvider = userDefinedFunctionProvider;
            return this;
        }

        @Nonnull
        public Builder withSerializableFunction(@Nonnull final UserDefinedFunction serializableFunction) {
            this.serializableFunction = serializableFunction;
            return this;
        }

        @Nonnull
        public Builder setPreparedParams(@Nonnull final PreparedParams preparedParams) {
            this.preparedParams = preparedParams;
            return this;
        }

        @Nonnull
        public Builder setTemporary(boolean isTemporary) {
            this.isTemporary = isTemporary;
            return this;
        }

        @Nonnull
        public RecordLayerInvokedRoutine build() {
            Assert.notNullUnchecked(name);
            Assert.notNullUnchecked(description);
            Assert.notNullUnchecked(userDefinedFunctionProvider);
            Assert.notNullUnchecked(serializableFunction);
            if (preparedParams == null) {
                preparedParams = PreparedParams.empty();
            }
            return new RecordLayerInvokedRoutine(description, normalizedDescription, name, preparedParams, isTemporary,
                    userDefinedFunctionProvider, serializableFunction);
        }
    }
}

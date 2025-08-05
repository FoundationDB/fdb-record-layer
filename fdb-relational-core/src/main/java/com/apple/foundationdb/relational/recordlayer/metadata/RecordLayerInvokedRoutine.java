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

import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
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
    private final String name;

    private final boolean isTemporary;
    private final boolean isCompiledSql;
    @Nonnull
    private final Function<Boolean, UserDefinedFunction> userDefinedRoutine;

    public RecordLayerInvokedRoutine(@Nonnull final String description,
                                     @Nonnull final String normalizedDescription,
                                     @Nonnull final String name,
                                     boolean isTemporary,
                                     boolean isCompiledSql,
                                     @Nonnull final Function<Boolean, UserDefinedFunction> userDefinedRoutine) {
        this.description = description;
        this.normalizedDescription = normalizedDescription;
        this.name = name;
        this.isTemporary = isTemporary;
        this.isCompiledSql = isCompiledSql;
        // TODO this used to be memoized
        this.userDefinedRoutine = userDefinedRoutine;
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
    public Function<Boolean, UserDefinedFunction> getUserDefinedFunctionSupplier() {
        return userDefinedRoutine;
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
        if (isCompiledSql) {
            return new RawSqlFunction(getName(), getDescription());
        } else {
            return userDefinedRoutine.apply(false);
        }
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
                .withUserDefinedRoutine(getUserDefinedFunctionSupplier(), isCompiledSql);
    }

    public static final class Builder {
        private String description;
        private String normalizedDescription;
        private String name;
        private Function<Boolean, UserDefinedFunction> userDefinedFunctionSupplier;
        private boolean isTemporary;
        private boolean isCompiledSql;

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
        public Builder withUserDefinedRoutine(@Nonnull final Function<Boolean, UserDefinedFunction> userDefinedFunctionSupplier, final boolean isCompiledSql) {
            this.userDefinedFunctionSupplier = userDefinedFunctionSupplier;
            this.isCompiledSql = isCompiledSql;
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
            // Assert.notNullUnchecked(description);
            Assert.thatUnchecked(userDefinedFunctionSupplier != null);
            return new RecordLayerInvokedRoutine(description, normalizedDescription, name, isTemporary, isCompiledSql, userDefinedFunctionSupplier);
        }
    }
}

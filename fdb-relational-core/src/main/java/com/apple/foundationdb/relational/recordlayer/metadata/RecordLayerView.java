/*
 * RecordLayerView.java
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

import com.apple.foundationdb.record.metadata.View;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a SQL view in the Record Layer metadata system.
 * Views are stored as raw SQL definitions and compiled lazily when referenced.
 */
public class RecordLayerView implements com.apple.foundationdb.relational.api.metadata.View {

    /**
     * The SQL query definition of the view (e.g., "SELECT * FROM employees WHERE salary > 50000").
     * This contains only the query portion, not the CREATE VIEW DDL statement.
     */
    @Nonnull
    private final String description;

    /**
     * The SQL name of the view as it appears in queries (e.g., "employee_view").
     */
    @Nonnull
    private final String name;

    /**
     * Whether this view is temporary and exists only for the duration of a transaction.
     */
    private final boolean isTemporary;

    /**
     * A function that compiles the view's SQL query into a logical operator tree.
     * The boolean parameter indicates whether to compile with the view with case-sensitive processing
     * of identifiers, see {@code Options.Name.CASE_SENSITIVE_IDENTIFIERS} for more information.
     */
    @Nonnull
    private final Function<Boolean, LogicalOperator> compilableViewSupplier;

    public RecordLayerView(@Nonnull final String description,
                           @Nonnull final String name,
                           final boolean isTemporary,
                           @Nonnull final Function<Boolean, LogicalOperator> compilableViewSupplier) {
        this.description = description;
        this.name = name;
        this.isTemporary = isTemporary;
        this.compilableViewSupplier = compilableViewSupplier;

    }

    @Nonnull
    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isTemporary() {
        return isTemporary;
    }

    @Nonnull
    public Function<Boolean, LogicalOperator> getCompilableViewSupplier() {
        return compilableViewSupplier;
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
    public View asRawView() {
        return new View(getName(), getDescription());
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RecordLayerView that = (RecordLayerView) o;
        return Objects.equals(description, that.description) && Objects.equals(name, that.name)
                && Objects.equals(isTemporary, that.isTemporary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, name, isTemporary);
    }

    @Override
    public String toString() {
        return "view (name '" + name + "', definition '" + description + "', temporary=" + isTemporary + ")";
    }

    @Nonnull
    public Builder toBuilder() {
        return newBuilder()
                .setName(getName())
                .setDescription(getDescription())
                .setTemporary(isTemporary())
                .setViewCompiler(getCompilableViewSupplier());
    }

    public static final class Builder {
        private String description;
        private String name;
        private boolean isTemporary;
        private Function<Boolean, LogicalOperator> compilableViewSupplier;

        private Builder() {
        }

        @Nonnull
        public Builder setDescription(@Nonnull final String description) {
            this.description = description;
            return this;
        }

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setTemporary(boolean isTemporary) {
            this.isTemporary = isTemporary;
            return this;
        }

        @Nonnull
        public Builder setViewCompiler(@Nonnull final Function<Boolean, LogicalOperator> compilableViewSupplier) {
            this.compilableViewSupplier = compilableViewSupplier;
            return this;
        }

        @Nonnull
        public RecordLayerView build() {
            Assert.notNullUnchecked(name);
            Assert.notNullUnchecked(description);
            Assert.notNullUnchecked(compilableViewSupplier);
            return new RecordLayerView(description, name, isTemporary, compilableViewSupplier);
        }
    }
}

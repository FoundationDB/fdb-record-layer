/*
 * ReferentialRecord.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A referential record is a {@link Type.Record} whose {@code name} is part of its identity for equality.
 *
 * A result we can use this class to encompass metadata structures such as {@code Table}s and {@code Index}es, and
 * at the same time, leverage record layer {@link Type} system to generate the internal representation, which is
 * currently, protobuf descriptors as a side effect.
 */
class ReferentialRecord extends Type.Record {

    @Nonnull
    private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashCode);

    protected ReferentialRecord(@Nonnull String name,
                                final boolean isNullable,
                                @Nullable final List<Field> fields) {
        super(name, isNullable, fields);
    }

    private int computeHashCode() {
        return Objects.hash(super.hashCode(), getName() == null ? 43 : getName().hashCode());
    }

    @Override
    public int hashCode() {
        return hashFunctionSupplier.get();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final var otherType = (Record) obj;
        return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                ((getName() == null && otherType.getName() == null) || (getName() != null && getName().equals(otherType.getName())) &&
                        (Objects.requireNonNull(getFields()).equals(otherType.getFields())));
    }

    @Nonnull
    public static ReferentialRecord fromFieldsWithName(@Nonnull String name,
                                                       final boolean isNullable,
                                                       @Nullable final List<Field> fields) {
        return new ReferentialRecord(name, isNullable, fields);
    }

    public static ReferentialRecord fromNamelessRecordType(@Nonnull String name, @Nonnull Type.Record record) {
        Assert.thatUnchecked(!(record instanceof ReferentialRecord));
        Assert.thatUnchecked(record.getName() == null || record.getName().equals(name));
        return new ReferentialRecord(name, record.isNullable(), record.getFields());
    }
}

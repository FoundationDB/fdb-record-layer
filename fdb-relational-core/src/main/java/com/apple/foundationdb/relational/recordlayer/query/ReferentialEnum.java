/*
 * ReferentialEnum.java
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

import com.google.common.base.Suppliers;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A referential enum is a {@link Type.Enum} whose {@code name} is part of its identity for equality. This
 * differs from the default implementation of {@link Type.Enum}, which relies on structural equality.
 * Note that a similar relationship exists between the default implementation of {@link Type.Record}
 * and {@link ReferentialRecord}. Like {@link ReferentialRecord}, this class should only be used when
 * translating the Relational DDL into the
 * {@link com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository}. Other calls (such
 * as for the query planner) should use the base class.
 */
class ReferentialEnum extends Type.Enum {
    @Nonnull
    private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashCode);

    public ReferentialEnum(boolean isNullable, @Nullable List<EnumValue> enumValues, @Nullable String name) {
        super(isNullable, enumValues, name);
    }

    private int computeHashCode() {
        return Objects.hash(super.hashCode(), getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }
        ReferentialEnum that = (ReferentialEnum) o;
        return super.equals(o) && Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return hashFunctionSupplier.get();
    }

    public static ReferentialEnum fromProtoWithName(boolean isNullable, Descriptors.EnumDescriptor protoDescriptor) {
        List<EnumValue> enumValues = Type.Enum.enumValuesFromProto(protoDescriptor.getValues());
        return new ReferentialEnum(isNullable, enumValues, protoDescriptor.getName());
    }
}

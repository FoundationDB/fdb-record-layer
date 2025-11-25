/*
 * ProtoUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.PlanHashable;
import com.google.protobuf.Internal;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.UUID;

/**
 * Utility functions for interacting with protobuf.
 */
public class ProtoUtils {

    private ProtoUtils() {
    }

    /**
     * Generates a JVM-wide unique type name.
     * @return a unique type name.
     */
    public static String uniqueTypeName() {
        return uniqueName("__type__");
    }

    /**
     * Generates a JVM-wide unique correlation name with a prefix.
     * @param prefix the type name prefix.
     * @return a unique type name prefixed with {@code prefix}.
     */
    public static String uniqueName(String prefix) {
        final var safeUuid = UUID.randomUUID().toString().replace('-', '_');
        return prefix + safeUuid;
    }

    /**
     * A dynamic enum when we don't want to worry about actual enum structures/descriptors etc.
     */
    public static class DynamicEnum implements Internal.EnumLite, PlanHashable {
        private final int number;
        @Nonnull
        private final String name;

        public DynamicEnum(final int number, @Nonnull final String name) {
            this.number = number;
            this.name = name;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Override
        public int getNumber() {
            return number;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DynamicEnum)) {
                return false;
            }
            final DynamicEnum that = (DynamicEnum)o;
            return number == that.number && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, name);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return getName();
        }
    }
}

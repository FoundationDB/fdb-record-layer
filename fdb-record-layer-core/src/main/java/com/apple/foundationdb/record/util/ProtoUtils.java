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
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.google.protobuf.Internal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Utility functions for interacting with protobuf.
 */
public class ProtoUtils {

    private static final String DOUBLE_UNDERSCORE_ESCAPE = "__0";
    private static final String DOLLAR_ESCAPE = "__1";
    private static final String DOT_ESCAPE = "__2";

    private static final List<String> INVALID_START_SEQUENCES = List.of(".", "$", DOUBLE_UNDERSCORE_ESCAPE, DOLLAR_ESCAPE, DOT_ESCAPE);

    private static final Pattern VALID_PROTOBUF_COMPLIANT_NAME_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    private ProtoUtils() {
    }

    @Nonnull
    public static String toProtoBufCompliantName(final String name) {
        if (INVALID_START_SEQUENCES.stream().anyMatch(name::startsWith)) {
            throw new InvalidNameException("name cannot start with " + INVALID_START_SEQUENCES);
        }
        String translated;
        if (name.startsWith("__")) {
            translated = "__" + translateSpecialCharacters(name.substring(2));
        } else {
            if (name.isEmpty()) {
                throw new InvalidNameException("name cannot be empty string");
            }
            translated = translateSpecialCharacters(name);
        }
        checkValidProtoBufCompliantName(translated);
        return translated;
    }

    public static void checkValidProtoBufCompliantName(String name) {
        if (!VALID_PROTOBUF_COMPLIANT_NAME_PATTERN.matcher(name).matches()) {
            throw new InvalidNameException(name + " it not a valid protobuf identifier");
        }
    }

    @Nonnull
    private static String translateSpecialCharacters(final String userIdentifier) {
        return userIdentifier.replace("__", DOUBLE_UNDERSCORE_ESCAPE).replace("$", DOLLAR_ESCAPE).replace(".", DOT_ESCAPE);
    }

    public static String toUserIdentifier(String protoIdentifier) {
        return protoIdentifier.replace(DOT_ESCAPE, ".").replace(DOLLAR_ESCAPE, "$").replace(DOUBLE_UNDERSCORE_ESCAPE, "__");
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

    @SuppressWarnings("serial")
    public static class InvalidNameException extends MetaDataException {
        public InvalidNameException(@Nonnull final String msg, @Nullable final Object... keyValues) {
            super(msg, keyValues);
        }
    }
}

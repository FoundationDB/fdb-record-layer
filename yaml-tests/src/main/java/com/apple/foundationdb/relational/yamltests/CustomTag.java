/*
 * CustomTag.java
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

package com.apple.foundationdb.relational.yamltests;

import javax.annotation.Nonnull;

public abstract class CustomTag {

    static final class Ignore extends CustomTag {
        static final Ignore INSTANCE = new Ignore();

        private Ignore() {
        }

        @Override
        public String toString() {
            return "!ignore";
        }
    }

    static final class StringContains {
        @Nonnull
        private final String value;

        StringContains(@Nonnull final String value) {
            this.value = value;
        }

        @Nonnull
        public String getValue() {
            return value;
        }

        @Nonnull
        public Matchers.ResultSetMatchResult matchWith(@Nonnull Object other, int rowNumber, @Nonnull String cellRef) {
            if (other instanceof String) {
                final var otherStr = (String) other;
                if (otherStr.contains(value)) {
                    return Matchers.ResultSetMatchResult.success();
                } else {
                    return Matchers.ResultSetMatchResult.fail(String.format("String mismatch at row: %d cellRef: %s%n The string '%s' does not contain '%s'", rowNumber, cellRef, otherStr, value));
                }
            } else {
                return Matchers.ResultSetMatchResult.fail(String.format("expected to match against a %s value, however we got %s which is %s", String.class.getSimpleName(), other, other.getClass().getSimpleName()));
            }
        }

        @Override
        public String toString() {
            return "!sc " + value;
        }
    }

    static final class NullPlaceholder {
        static final NullPlaceholder INSTANCE = new NullPlaceholder();

        private NullPlaceholder() {
        }

        @Override
        public String toString() {
            return "!null";
        }
    }

    static final class NotNull {
        static final NotNull INSTANCE = new NotNull();

        private NotNull() {
        }

        @Override
        public String toString() {
            return "!not_null";
        }
    }
}

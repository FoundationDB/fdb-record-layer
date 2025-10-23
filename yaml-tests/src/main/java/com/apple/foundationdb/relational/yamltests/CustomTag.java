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

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.relational.util.Assert;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;

import javax.annotation.Nonnull;
import java.util.UUID;

@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
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
                    return Matchers.ResultSetMatchResult.fail("String mismatch at row: " + rowNumber + " cellRef: " + cellRef + "\n The string '" + otherStr + "' does not contain '" + value + "'");
                }
            } else {
                return Matchers.ResultSetMatchResult.fail("expected to match against a " + String.class.getSimpleName() + " value, however we got " + other + " which is " + other.getClass().getSimpleName());
            }
        }

        @Override
        public String toString() {
            return "!sc " + value;
        }
    }

    static final class UuidField {
        @Nonnull
        private final String value;

        UuidField(@Nonnull final String value) {
            this.value = value;
        }

        @Nonnull
        public String getValue() {
            return value;
        }

        @Nonnull
        public Matchers.ResultSetMatchResult matchWith(@Nonnull Object other, int rowNumber, @Nonnull String cellRef) {
            UUID otherUUID;
            try {
                otherUUID = UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                return Matchers.ResultSetMatchResult.fail("Provided string is not a valid UUID format at row: " + rowNumber + " cellRef: " + cellRef + "\n. Provided String '" + value + "'");

            }
            if (other instanceof UUID) {
                if (otherUUID.equals(UUID.fromString(value))) {
                    return Matchers.ResultSetMatchResult.success();
                }
            }
            // not matched
            return Matchers.ResultSetMatchResult.fail("UUID mismatch at row: " + rowNumber + " cellRef: " + cellRef + "\n The actual '" + other + "' does not match UUID '" + value + "'");
        }

        @Override
        public String toString() {
            return "!uuid " + value;
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

    static final class Vector16Field {
        @Nonnull
        private final SequenceNode yamlNode;

        HalfRealVector vector;

        Vector16Field(@Nonnull final Node node) {
            this.yamlNode = Assert.castUnchecked(node, SequenceNode.class);
        }

        @Nonnull
        public Matchers.ResultSetMatchResult matchWith(@Nonnull Object other, int rowNumber, @Nonnull String cellRef) {
            HalfRealVector thisVector = (HalfRealVector)Matchers.constructVectorFromString(16, Assert.castUnchecked(yamlNode, SequenceNode.class));
            if (other instanceof HalfRealVector) {
                if (other.equals(thisVector)) {
                    return Matchers.ResultSetMatchResult.success();
                }
            }
            // not matched
            return Matchers.ResultSetMatchResult.fail("vector mismatch at row " + rowNumber + ", " + cellRef + " expected: " + this + ", got: " + other);
        }

        @Override
        public String toString() {
            return prettyPrintYamlNode(yamlNode);
        }

        @Nonnull
        private static String prettyPrintYamlNode(@Nonnull final SequenceNode sequenceNode) {
            final var stringBuilder = new StringBuilder();
            stringBuilder.append(sequenceNode.getTag()).append(" [");
            sequenceNode.getValue().forEach(v -> stringBuilder.append(Assert.castUnchecked(v, ScalarNode.class).getValue()));
            stringBuilder.append("]");
            return stringBuilder.toString();
        }
    }
}

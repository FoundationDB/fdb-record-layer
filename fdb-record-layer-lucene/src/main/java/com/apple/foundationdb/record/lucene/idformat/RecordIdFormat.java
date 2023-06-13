/*
 * RecordIdFormat.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.idformat;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RecordIdFormat {
    public enum FormatElementType implements FormatElement {
        NONE(0),
        NULL(1),
        INT32(5),
        INT32_OR_NULL(5),
        INT64(9),
        INT64_OR_NULL(9),
        UUID_AS_STRING(18),
        STRING_16(50);

        // The required size (in bytes) when this subtype is serialized
        // This is to ensure we always use the maximum allocated size so that the final size is fixed
        private final int allocatedSize;

        FormatElementType(final int allocatedSize) {
            this.allocatedSize = allocatedSize;
        }

        public int getAllocatedSize() {
            return allocatedSize;
        }
    }

    @Nonnull
    private final TupleElement element;

    public RecordIdFormat(@Nonnull final TupleElement element) {
        this.element = element;
    }

    public static RecordIdFormat of(FormatElement ...elements) {
        return new RecordIdFormat(TupleElement.of(elements));
    }

    public TupleElement getElement() {
        return element;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordIdFormat)) {
            return false;
        }
        final RecordIdFormat that = (RecordIdFormat)o;
        return element.equals(that.element);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element);
    }

    @Override
    public String toString() {
        return element.toString();
    }

    public interface FormatElement {
    }

    public static class TupleElement implements FormatElement {
        @Nonnull
        private final List<FormatElement> children;

        public TupleElement(@Nonnull final List<FormatElement> children) {
            if (children.isEmpty()) {
                throw new RecordCoreFormatException("Tuple format element cannot be empty");
            }
            this.children = children;
        }

        @Nonnull
        public List<FormatElement> getChildren() {
            return children;
        }

        @Nonnull
        public static TupleElement of(FormatElement...children) {
            return new TupleElement(List.of(children));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TupleElement)) {
                return false;
            }
            final TupleElement that = (TupleElement)o;
            return children.equals(that.children);
        }

        @Override
        public int hashCode() {
            return Objects.hash(children);
        }

        @Override
        public String toString() {
            return "[" + getChildren().stream().map(Objects::toString).collect(Collectors.joining(",")) + "]";
        }
    }
}

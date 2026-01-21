/*
 * YamsqlReference.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * This represents a location in a YAMSQL file.
 */
public class YamsqlReference implements Comparable<YamsqlReference> {
    @Nonnull
    private final YamsqlResource resource;
    private final int lineNumber;
    @Nonnull
    private final Supplier<Tuple> tupleSupplier;


    private YamsqlReference(@Nonnull final YamsqlResource resource, int lineNumber) {
        this.resource = resource;
        this.lineNumber = lineNumber;
        this.tupleSupplier = () -> Tuple.fromList(resource.tupleSupplier.get().getItems()).add(lineNumber);
    }

    @Nonnull
    public YamsqlResource getResource() {
        return resource;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Returns the call stack of this {@link YamsqlReference}.
     * @return a list of locations in the call stack of this {@link YamsqlReference}.
     */
    public ImmutableList<YamsqlReference> getCallStack() {
        final var builder = ImmutableList.<YamsqlReference>builder();
        var current = this;
        while (current != null) {
            builder.add(current);
            current = current.getResource().parentRef;
        }
        return builder.build().reverse();
    }

    public YamsqlResource newResource(@Nonnull String path) {
        return new YamsqlResource(this, path);
    }

    @Override
    @Nonnull
    public String toString() {
        return (getResource().parentRef == null ? "" : getResource().parentRef + " > ") + getResource().getFileName() + ":" + getLineNumber();
    }

    @Override
    public boolean equals(Object object) {
        if (object == null) {
            return false;
        }
        if (!(object instanceof YamsqlReference)) {
            return false;
        }
        final var otherReference = (YamsqlReference) object;
        return getResource().equals(otherReference.getResource()) && getLineNumber() == otherReference.getLineNumber();
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, lineNumber);
    }

    @Override
    public int compareTo(final YamsqlReference o) {
        return tupleSupplier.get().compareTo(o.tupleSupplier.get());
    }

    /**
     * A resource represents path to a YAMSQL file. However, this class also serves the purpose of maintaining the
     * parent {@link YamsqlReference}s that (recursively) provides information about the calling stack that has lead to
     * this file being executed.
     */
    public static class YamsqlResource {
        @Nullable
        private final YamsqlReference parentRef;
        @Nonnull
        private final String path;
        @Nonnull
        private final Supplier<Tuple> tupleSupplier;

        private YamsqlResource(@Nullable final YamsqlReference parentRef, @Nonnull final String path) {
            if (parentRef != null) {
                assertNotCyclic(parentRef, path);
            }
            this.parentRef = parentRef;
            this.path = path;
            this.tupleSupplier = () -> parentRef == null ? Tuple.from(path) : Tuple.fromList(parentRef.tupleSupplier.get().getItems()).add(path);
        }

        public YamsqlReference withLineNumber(int lineNumber) {
            return new YamsqlReference(this, lineNumber);
        }

        public boolean isTopLevel() {
            return parentRef == null;
        }

        @Nullable
        public YamsqlReference getParentRef() {
            return parentRef;
        }

        @Nonnull
        public String getPath() {
            return path;
        }

        @Override
        @Nonnull
        public String toString() {
            return path + ((parentRef == null) ? "" : " via (" + parentRef + ")");
        }

        @Override
        public boolean equals(Object object) {
            if (object == null) {
                return false;
            }
            if (!(object instanceof YamsqlResource)) {
                return false;
            }
            final var otherResource = (YamsqlResource) object;
            if (parentRef == null) {
                if (otherResource.parentRef != null) {
                    return false;
                }
                return path.equals(otherResource.path);
            }
            return parentRef.equals(otherResource.parentRef) && path.equals(otherResource.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parentRef, path);
        }

        public static YamsqlResource base(@Nonnull final String path) {
            return new YamsqlResource(null, path);
        }

        public String getFileName() {
            String fileName;
            if (path.contains("/")) {
                final String[] split = path.split("/");
                fileName = split[split.length - 1];
            } else {
                fileName = path;
            }
            return fileName;
        }

        private static void assertNotCyclic(@Nonnull final YamsqlReference parentRef, @Nonnull final String path) {
            final var asTuple = parentRef.tupleSupplier.get().getItems();
            Assert.thatUnchecked(asTuple.stream().noneMatch(path::equals), "Cyclic path detected at: " + parentRef);
        }
    }
}

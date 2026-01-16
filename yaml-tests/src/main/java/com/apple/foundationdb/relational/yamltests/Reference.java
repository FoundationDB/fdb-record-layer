/*
 * Reference.java
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
public class Reference implements Comparable<Reference> {
    @Nonnull
    private final Resource resource;
    private final int lineNumber;
    @Nonnull
    private final Supplier<Tuple> tupleSupplier;


    private Reference(@Nonnull final Resource resource, int lineNumber) {
        this.resource = resource;
        this.lineNumber = lineNumber;
        this.tupleSupplier = () -> Tuple.from(resource.tupleSupplier.get().getItems()).add(lineNumber);
    }

    @Nonnull
    public Resource getResource() {
        return resource;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Returns the call stack of this {@link Reference}.
     * @return a list of locations in the call stack of this {@link Reference}.
     */
    public ImmutableList<Reference> getCallStack() {
        final var builder = ImmutableList.<Reference>builder();
        var current = this;
        while (current != null) {
            builder.add(current);
            current = current.getResource().parentRef;
        }
        return builder.build().reverse();
    }

    public Resource newResource(@Nonnull String path) {
        return Resource.with(this, path);
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
        if (!(object instanceof Reference)) {
            return false;
        }
        final var otherReference = (Reference) object;
        return getResource().equals(otherReference.getResource()) && getLineNumber() == otherReference.getLineNumber();
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, lineNumber);
    }

    @Override
    public int compareTo(final Reference o) {
        return tupleSupplier.get().compareTo(o.tupleSupplier.get());
    }

    /**
     * A resource represents path to a YAMSQL file. However, this class also serves the purpose of maintaining the
     * parent {@link Reference}s that (recursively) provides information about the calling stack that has lead to
     * this file being executed.
     */
    public static class Resource {
        @Nullable
        private final Reference parentRef;
        @Nonnull
        private final String path;
        @Nonnull
        private final Supplier<Tuple> tupleSupplier;

        private Resource(@Nullable final Reference parentRef, @Nonnull final String path) {
            this.parentRef = parentRef;
            this.path = path;
            this.tupleSupplier = () -> parentRef == null ? Tuple.from(path) : Tuple.fromList(parentRef.tupleSupplier.get().getItems()).add(path);
        }

        public Reference withLineNumber(int lineNumber) {
            return new Reference(this, lineNumber);
        }

        public boolean isTopLevel() {
            return parentRef == null;
        }

        @Nullable
        public Reference getParentRef() {
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
            if (!(object instanceof Resource)) {
                return false;
            }
            final var otherResource = (Resource) object;
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

        public static Resource base(@Nonnull final String path) {
            return new Resource(null, path);
        }

        public static Resource with(@Nonnull final Reference parentRef, @Nonnull final String path) {
            assertNotCyclic(parentRef, path);
            return new Resource(parentRef, path);
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

        private static void assertNotCyclic(@Nonnull final Reference parentRef, @Nonnull final String path) {
            final var asTuple = parentRef.tupleSupplier.get().getItems();
            Assert.thatUnchecked(asTuple.stream().noneMatch(path::equals), "Cyclic path detected at: " + parentRef);
        }
    }
}

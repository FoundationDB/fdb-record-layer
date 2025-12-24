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

import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This represents a location in a YAMSQL file.
 */
public class Reference {

    @Nonnull
    private final Resource resource;

    private final int lineNumber;

    private Reference(@Nonnull final Resource resource, int lineNumber) {
        this.resource = resource;
        this.lineNumber = lineNumber;
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
     * @return a list of locations in the call stack of this {@link Reference}. However, the list is inverted, that is,
     * the first caller is the last element in this list. Each element of the list is a pair of a YAMSQL file and the
     * line number
     */
    public ImmutableList<NonnullPair<String, Integer>> getCallStack() {
        final var builder = ImmutableList.<NonnullPair<String, Integer>>builder();
        var current = this;
        while (current != null) {
            builder.add(NonnullPair.of(getFileName(current.getResource().getPath()), current.getLineNumber()));
            current = current.getResource().parentRef;
        }
        return builder.build();
    }

    public Resource newResource(@Nonnull String path) {
        return Resource.with(this, path);
    }

    private static String getFileName(@Nonnull String path) {
        String fileName;
        if (path.contains("/")) {
            final String[] split = path.split("/");
            fileName = split[split.length - 1];
        } else {
            fileName = path;
        }
        return fileName;
    }


    @Override
    @Nonnull
    public String toString() {
        return (getResource().parentRef == null ? "" : getResource().parentRef + " > ") + getFileName(getResource().getPath()) + ":" + getLineNumber();
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

        private Resource(@Nullable final Reference parentRef, @Nonnull final String path) {
            this.parentRef = parentRef;
            this.path = path;
        }

        public Reference withLineNumber(int lineNumber) {
            return new Reference(this, lineNumber);
        }

        public boolean isTopLevel() {
            return parentRef == null;
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
            checkIfNotCyclic(parentRef, path);
            return new Resource(parentRef, path);
        }

        private static void checkIfNotCyclic(@Nonnull final Reference parentRef, @Nonnull final String path) {
            var currentRef = parentRef;
            while (currentRef != null) {
                Assert.thatUnchecked(!currentRef.getResource().getPath().equals(path), "Cyclic path detected with: " + path);
                currentRef = currentRef.getResource().parentRef;
            }
        }
    }
}

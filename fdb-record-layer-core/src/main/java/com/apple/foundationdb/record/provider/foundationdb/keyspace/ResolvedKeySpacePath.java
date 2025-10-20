/*
 * ResolvedKeySpacePath.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A <code>KeySpacePath</code> that has been resolved into the value that will be physically stored to
 * represent the path element in the FDB keyspace.  A {@link KeySpacePath} represents a path through a
 * logical directory tree defined by a {@link KeySpaceDirectory}.  Some directories, such as a the
 * {@link DirectoryLayerDirectory} logically represent their values in one format (such as a string)
 * but store their version in another "resolved" format (such as a long).  A <code>ResolvedKeySpacePath</code>
 * represents the value for a path entry as resolved from a <code>KeySpacePath</code>.
 */
public class ResolvedKeySpacePath {
    @Nullable
    private final ResolvedKeySpacePath parent;
    @Nonnull
    private final KeySpacePath inner;
    @Nonnull
    private final PathValue value;
    @Nullable
    private final Tuple remainder;

    @Nullable
    private Tuple cachedTuple;
    @Nullable
    private Subspace cachedSubspace;

    /**
     * Create a resolved keyspace path.
     *
     * @param parent the parent resolved path
     * @param inner the path for which the value was resolved
     * @param value the resolved value
     * @param remainder if the path was resolved from a <code>Tuple</code> this is the remaining portion that
     *   extends beyond the end of the path.
     */
    protected ResolvedKeySpacePath(@Nullable ResolvedKeySpacePath parent, @Nonnull KeySpacePath inner,
                                   @Nonnull PathValue value, @Nullable Tuple remainder) {
        this.parent = parent;
        this.inner = inner;
        this.value = value;
        this.remainder = remainder;
    }

    /**
     * Get the length of the path.
     * @return the length of the path
     */
    public int size() {
        return inner.size();
    }

    /**
     * Returns the parent path element.
     * @return the parent path element or <code>null</code> if this is the root of the path
     */
    @Nullable
    public ResolvedKeySpacePath getParent() {
        return parent;
    }

    /**
     * Returns the directory name for this path element.
     * @return the directory name
     */
    @Nonnull
    public String getDirectoryName() {
        return inner.getDirectoryName();
    }

    /**
     * Returns the directory that corresponds to this path entry.
     * @return returns the directory that corresponds to this path entry
     */
    @Nonnull
    public KeySpaceDirectory getDirectory() {
        return inner.getDirectory();
    }

    /**
     * Returns the logical value (prior to resolving) for this path element.
     *
     * @return the logical value for this path element
     */
    @Nullable
    public Object getLogicalValue() {
        return inner.getValue();
    }

    /**
     * Returns the value that will be physically stored to represent this path element along with any metadata that
     * may be associated with it.
     *
     * @return the value that will be stored stored for the path element
     */
    @Nonnull
    public PathValue getResolvedPathValue() {
        return value;
    }

    /**
     * Returns the value that will be physically stored to represent this path element.
     *
     * @return the value that will be stored stored for the path element
     */
    @Nullable
    public Object getResolvedValue() {
        return value.getResolvedValue();
    }

    /**
     * Returns any metadata that may be stored along with the resolved value for this path element. Metadata is
     * produced from {@link DirectoryLayerDirectory} entries in which the {@link LocatableResolver} that is
     * performing the string to long mapping is also configured to store additional metadata associated with
     * that entry.
     *
     * @return the metadata that is stored along with the resolved value for this path element or <code>null</code>
     *   if there is no metadata
     */
    @Nullable
    public byte[] getResolvedMetadata() {
        return value.getMetadata();
    }

    /**
     * Converts the resolved path into a <code>Tuple</code>.
     *
     * @return the <code>Tuple</code> form of the resolved path
     */
    @Nonnull
    public Tuple toTuple() {
        if (cachedTuple == null) {
            final int len = size();

            final Object[] values = new Object[len];
            ResolvedKeySpacePath current = this;
            for (int i = len - 1; i >= 0; i--) {
                values[i] = current.getResolvedValue();
                current = current.getParent();
            }
            cachedTuple = Tuple.from(values);
        }
        return cachedTuple;
    }

    @Nonnull
    public Subspace toSubspace() {
        if (cachedSubspace == null) {
            cachedSubspace = new Subspace(toTuple().pack());
        }
        return cachedSubspace;
    }

    /**
     * Convert the resolved path back into a {@link KeySpacePath}.
     *
     * @return The <code>KeySpacePath</code> for this resolved path
     */
    @Nonnull
    public KeySpacePath toPath() {
        return inner;
    }

    /**
     * If this path was created via {@link KeySpace#resolveFromKey(FDBRecordContext, Tuple)}, this returns
     * any remaining portion of the input tuple that was not used to construct the path.
     * @return the remaining portion of the original input tuple or <code>null</code>
     */
    @Nullable
    public Tuple getRemainder() {
        return remainder;
    }

    /**
     * Flattens the path into a list of <code>ResolvedKeySpacePath</code> entries, with the root of the path
     * located at position 0.
     *
     * @return this path as a list
     */
    @Nonnull
    public List<ResolvedKeySpacePath> flatten() {
        final int len = size();
        final ResolvedKeySpacePath[] flat = new ResolvedKeySpacePath[len];
        ResolvedKeySpacePath current = this;
        for (int i = len - 1; i >= 0; i--) {
            flat[i] = current;
            current = current.getParent();
        }
        return Arrays.asList(flat);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof ResolvedKeySpacePath)) {
            return false;
        }

        ResolvedKeySpacePath otherPath = (ResolvedKeySpacePath) other;
        return Objects.equals(this.getResolvedPathValue(), otherPath.getResolvedPathValue()) &&
                Objects.equals(this.getParent(), otherPath.getParent()) &&
                this.inner.equals(otherPath.inner) &&
                Objects.equals(this.remainder, otherPath.remainder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inner, getResolvedPathValue(), remainder, getParent());
    }

    @Override
    public String toString() {
        final List<ResolvedKeySpacePath> path = flatten();
        final StringBuilder sb = new StringBuilder();
        for (ResolvedKeySpacePath current : path) {
            sb.append('/').append(current.getDirectoryName()).append(':');
            appendValue(sb, current.getLogicalValue());
            if (!Objects.equals(current.getLogicalValue(), current.getResolvedValue())) {
                sb.append('[');
                appendValue(sb, current.getResolvedValue());
                sb.append(']');
            }
            if (current.getRemainder() != null) {
                sb.append('+').append(current.getRemainder());
            }
        }
        return sb.toString();
    }

    public static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            sb.append('"').append(value).append('"');
        } else if (value instanceof byte[]) {
            sb.append("0x");
            sb.append(ByteArrayUtil2.toHexString((byte[])value));
        } else {
            sb.append(value);
        }
    }

    /**
     * Returns a new {@code ResolvedKeySpacePath} that is the same, except with the provided {@link #getRemainder()}.
     * @param newRemainder a new remainder. This can be {@code null} to remove the remainder entirely.
     * @return a new {@code ResolvedKeySpacePath} that is the same as this, except with a different {@link #getRemainder()}.
     */
    @Nonnull
    public ResolvedKeySpacePath withRemainder(@Nullable final Tuple newRemainder) {
        // this could probably copy the cachedTuple & cachedSubspace
        return new ResolvedKeySpacePath(parent, inner, value, newRemainder);
    }
}

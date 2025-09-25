/*
 * PathValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A class to represent the value stored at a particular element of a {@link KeySpacePath}. The <code>resolvedValue</code>
 * is the object that will appear in the {@link com.apple.foundationdb.tuple.Tuple} when
 * {@link KeySpacePath#toTuple(com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext)} is invoked.
 * The <code>metadata</code> is left null by {@link KeySpaceDirectory} but other implementations may make use of
 * it (e.g. {@link DirectoryLayerDirectory}).
 */
@API(API.Status.UNSTABLE)
public class PathValue {
    @Nullable
    private Object resolvedValue;
    @Nullable
    private byte[] metadata;

    PathValue(@Nullable Object resolvedValue) {
        this(resolvedValue, null);
    }

    PathValue(@Nullable Object resolvedValue, @Nullable byte[] metadata) {
        this.resolvedValue = resolvedValue;
        this.metadata = metadata == null ? null : Arrays.copyOf(metadata, metadata.length);
    }

    /**
     * Returns the value that will be stored in the FDB row key for a path.
     * @return the value that will be stored in the FDB row key
     */
    @Nullable
    public Object getResolvedValue() {
        return resolvedValue;
    }

    /**
     * If the <code>PathValue</code> was returned by the {@link DirectoryLayerDirectory} or some other directory
     * type that involves a {@link LocatableResolver}, this will return any metadata that is associated with the
     * key in by <code>LocatableResolver</code>.
     *
     * @return metadata associated with the key in the <code>LocatableResolver</code> or <code>null</code> if
     *   no metadata exists or is applicable for the directory type
     */
    @Nullable
    public byte[] getMetadata() {
        return metadata == null ? null : Arrays.copyOf(metadata, metadata.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PathValue)) {
            return false;
        }
        PathValue that = (PathValue) other;
        return KeySpaceDirectory.areEqual(this.resolvedValue, that.resolvedValue) &&
                Arrays.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(KeySpaceDirectory.valueHashCode(resolvedValue), Arrays.hashCode(metadata));
    }
}

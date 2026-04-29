/*
 * OnWriteListener.java
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.Range;

import javax.annotation.Nonnull;

/**
 * Interface for call backs whenever we write data to the database.
 */
public interface OnKeyValueWriteListener {
    /**
     * Callback method that is invoked for each key/value pair that is written to the database.
     * <p>
     * This is a default method and its base implementation is a no-op. Implementors of the interface can override this
     * method to react to the deletion of a neighbor node, for example, to clean up related resources or update internal
     * state.
     * @param layer the layer the data was written to
     * @param key the key
     * @param value the value.
     */
    @SuppressWarnings("unused")
    default void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
        // nothing
    }

    /**
     * Callback method invoked when a key is deleted.
     * <p>
     * This is a default method and its base implementation is a no-op. Implementors of the interface can override this
     * method to react to the deletion of a neighbor node, for example, to clean up related resources or update internal
     * state.
     * @param layer the layer index where the deletion occurred
     * @param key the key that was deleted
     */
    @SuppressWarnings("unused")
    default void onKeyDeleted(final int layer, @Nonnull final byte[] key) {
        // nothing
    }

    /**
     * Callback method invoked when an entire range is deleted.
     * <p>
     * This is a default method and its base implementation is a no-op. Implementors of the interface can override this
     * method to react to the deletion of a neighbor node, for example, to clean up related resources or update internal
     * state.
     * @param layer the layer index where the deletion occurred
     * @param range the {@link Range} that was deleted
     */
    @SuppressWarnings("unused")
    default void onRangeDeleted(final int layer, @Nonnull final Range range) {
        // nothing
    }
}

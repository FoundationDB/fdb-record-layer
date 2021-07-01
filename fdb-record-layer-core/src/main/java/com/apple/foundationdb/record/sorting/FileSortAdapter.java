/*
 * FileSortAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;

/**
 * Provide various options for {@link FileSorter} and {@link FileSortCursor}.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public interface FileSortAdapter<K, V> extends MemorySortAdapter<K, V> {
    /**
     * Get the name of a file to use for saving sorted records.
     * @return a file into which to save sorted records
     * @throws IOException if something fails creating the file
     */
    @Nonnull
    File generateFilename() throws IOException;

    /**
     * Write the value to a Protobuf stream.
     * @param value the sorted value
     * @param stream the destination stream
     * @throws IOException if something fails writing to a file
     */
    void writeValue(@Nonnull V value, @Nonnull CodedOutputStream stream) throws IOException;

    /**
     * Read the value from a Protobuf stream.
     * @param stream the source stream
     * @return the next value in the stream
     * @throws IOException if something fails reading from the file
     */
    @Nonnull
    V readValue(@Nonnull CodedInputStream stream) throws IOException;

    /**
     * Get the mimimum number of records needed to write a file.
     * If loading finds fewer than this number of records, the results are kept in memory and returned from there.
     * @return minimum number of records
     */
    int getMinFileSize();

    /**
     * Get the maximum number of files to keep before merging.
     * @return the maximum number of files kept by a single cursor
     */
    int getMaxNumFiles();

    /**
     * Get the maximum number of files in a section.
     * Sections allow skipping through the file faster.
     * @return the number of records in each section
     */
    int getRecordsPerSection();

    // TODO: Limit on number of records total?

    /**
     * Get whether files should be compressed.
     * @return {@code true} if files are compressed
     */
    boolean isCompressed();

    /**
     * Get whether files should be encrypted.
     * @return encryption key to use or {@code null} for unencrypted files
     */
    @Nullable
    Key getEncryptionKey();

    /**
     * Get source of randomness for encryption.
     * @return secure random source
     */
    @Nullable
    SecureRandom getSecureRandom();
}
